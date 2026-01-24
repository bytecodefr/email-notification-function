import crypto from 'crypto';
import { Client, Databases, Users, Messaging, ID } from 'node-appwrite';

const STATUS_LABELS = {
  draft: 'Draft',
  submitted: 'Submitted',
  in_review: 'In Review',
  approved: 'Approved',
  rejected: 'Rejected',
  needs_action: 'Needs Action'
};

const NOTIFIABLE_STATUSES = new Set(['in_review', 'approved', 'rejected', 'needs_action']);

const APPLICATION_COLLECTION_IDS = {
  applicationForms: process.env.APPLICATION_FORMS_COLLECTION_ID || 'application_forms',
  nationalIds: process.env.NATIONAL_ID_COLLECTION_ID || 'national_id_applications',
  businesses: process.env.BUSINESS_COLLECTION_ID || 'business_registrations',
  gunLicenses: process.env.GUN_LICENSE_COLLECTION_ID || 'gun_licenses'
};

const APPLICATION_COLLECTIONS = {
  [APPLICATION_COLLECTION_IDS.applicationForms]: {
    source: 'applications',
    label: 'Government Application'
  },
  [APPLICATION_COLLECTION_IDS.nationalIds]: {
    source: 'nationalIds',
    label: 'National ID Application'
  },
  [APPLICATION_COLLECTION_IDS.businesses]: {
    source: 'businesses',
    label: 'Business Registration'
  },
  [APPLICATION_COLLECTION_IDS.gunLicenses]: {
    source: 'gunLicenses',
    label: 'Gun License Application'
  }
};

const PAY_STUBS_COLLECTION_ID = process.env.PAY_STUBS_COLLECTION_ID || 'pay_stubs';
const EMPLOYEES_COLLECTION_ID = process.env.EMPLOYEES_COLLECTION_ID || 'employees';

const DEFAULT_THROTTLE_MINUTES = 10;

// Environment variable: NOTIFY_THROTTLE_MINUTES

const parseJson = (value) => {
  if (!value) return null;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (err) {
    return null;
  }
};

const parseBoolean = (value) => {
  if (value === true || value === false) return value;
  if (value === null || value === undefined) return false;
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
};

const normalizeStatus = (value = '') => {
  const status = String(value || '').toLowerCase();
  if (status === 'pending' || status === 'in-review') return 'in_review';
  if (status === 'action_required') return 'needs_action';
  if (status === 'new') return 'submitted';
  return status || 'submitted';
};

const titleCase = (value = '') =>
  String(value)
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .split(' ')
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ');

const normalizeText = (value) => {
  if (value === null || value === undefined) return null;
  const trimmed = String(value).trim();
  return trimmed.length ? trimmed : null;
};

const escapeHtml = (value) =>
  String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/'/g, '&#39;');

const hashPayload = (payload) =>
  crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex');

const maskEmail = (email) => {
  const value = String(email || '').trim();
  const parts = value.split('@');
  if (parts.length !== 2) return 'unknown';
  const name = parts[0];
  const domain = parts[1];
  if (!name) return `***@${domain}`;
  if (name.length === 1) return `${name}***@${domain}`;
  const masked = `${name.slice(0, 1)}***${name.slice(-1)}`;
  return `${masked}@${domain}`;
};

const parseDate = (value) => {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const getEventName = (req, payload) => {
  const headerEvent = req.headers?.['x-appwrite-event'] || req.headers?.['X-Appwrite-Event'];
  if (headerEvent) return headerEvent;
  const headerEvents = req.headers?.['x-appwrite-events'] || req.headers?.['X-Appwrite-Events'];
  if (headerEvents && typeof headerEvents === 'string') {
    return headerEvents.split(',')[0];
  }
  if (Array.isArray(payload?.events) && payload.events.length > 0) {
    return payload.events[0];
  }
  return payload?.event || null;
};

const guessEventType = (eventName, document) => {
  if (eventName) {
    const parts = eventName.split('.');
    return parts[parts.length - 1];
  }
  if (document?.$createdAt && document?.$updatedAt && document.$createdAt === document.$updatedAt) {
    return 'create';
  }
  return 'update';
};

const buildPortalLink = (baseUrl, path) => {
  const trimmedBase = String(baseUrl || '').replace(/\/+$/, '');
  const trimmedPath = String(path || '').replace(/^\/+/, '');
  return trimmedBase && trimmedPath ? `${trimmedBase}/${trimmedPath}` : trimmedBase || '';
};

const getApplicantName = (doc) => {
  if (!doc) return null;
  const fullName = normalizeText(doc.fullName);
  if (fullName) return fullName;
  const nameParts = [doc.firstName, doc.middleName, doc.lastName, doc.suffix]
    .map(normalizeText)
    .filter(Boolean);
  if (nameParts.length) return nameParts.join(' ');
  const businessName = normalizeText(doc.businessName || doc.ownerName || doc.responsiblePersonName);
  return businessName || null;
};

const getReference = (doc) =>
  normalizeText(doc?.referenceNumber) ||
  normalizeText(doc?.nationalIdNumber) ||
  (doc?.$id ? doc.$id.slice(0, 10).toUpperCase() : null) ||
  (doc?.id ? String(doc.id).slice(0, 10).toUpperCase() : null) ||
  'Not provided';

const buildApplicationFingerprint = ({ status, adminNotes, needsActionNote, rejectionReason }) =>
  hashPayload({ status, adminNotes, needsActionNote, rejectionReason });

const buildNotificationType = ({ status, adminNotes, needsActionNote, rejectionReason }) => {
  const parts = [];
  if (status) parts.push(`status:${status}`);
  if (adminNotes) parts.push('admin_note');
  if (needsActionNote) parts.push('needs_action_note');
  if (rejectionReason) parts.push('rejection_reason');
  return parts.join('|') || 'status';
};

const buildApplicationEmail = ({
  applicationLabel,
  statusLabel,
  reference,
  adminNotes,
  needsActionNote,
  rejectionReason,
  link,
  name
}) => {
  const isActionRequired = statusLabel === STATUS_LABELS.needs_action;
  const isApproved = statusLabel === STATUS_LABELS.approved;
  const isRejected = statusLabel === STATUS_LABELS.rejected;
  const isInReview = statusLabel === STATUS_LABELS.in_review;

  const subject = isActionRequired
    ? `Action Required: ${applicationLabel} - Reference ${reference}`
    : isApproved
      ? `Application Approved: ${applicationLabel} - Reference ${reference}`
      : isRejected
        ? `Application Status Update: ${applicationLabel} - Reference ${reference}`
        : `Status Update: ${applicationLabel} - Reference ${reference}`;

  const safeName = escapeHtml(name);
  const safeLabel = escapeHtml(applicationLabel);
  const safeReference = escapeHtml(reference);
  const safeStatus = escapeHtml(statusLabel);
  const safeLink = escapeHtml(link);

  // Status-specific messaging - STATUS TAKES PRECEDENCE
  let statusMessage = '';
  let nextSteps = '';

  if (isActionRequired) {
    statusMessage = `We require additional information or action from you to continue processing your ${safeLabel}. Please review the details below and take the necessary steps at your earliest convenience to avoid delays in processing.`;
    nextSteps = `
      <div style="background:#fffbeb;border:2px solid #f59e0b;padding:20px;margin:24px 0;">
        <p style="margin:0 0 12px;font-weight:700;color:#78350f;font-size:15px;">ACTION REQUIRED</p>
        <p style="margin:0;color:#78350f;font-size:14px;line-height:1.6;">Please log in to your portal and address the items mentioned in the administrator notes below. Prompt action will help avoid delays in processing your application.</p>
      </div>`;
  } else if (isApproved) {
    statusMessage = `We are pleased to inform you that your ${safeLabel} has been approved. You may now proceed with the next steps as outlined in your application.`;
    nextSteps = `
      <div style="background:#f0fdf4;border:2px solid #22c55e;padding:20px;margin:24px 0;">
        <p style="margin:0 0 12px;font-weight:700;color:#166534;font-size:15px;">NEXT STEPS</p>
        <p style="margin:0;color:#166534;font-size:14px;line-height:1.6;">Please log in to your portal to view complete details and download any necessary documents. Further instructions are available in your application dashboard.</p>
      </div>`;
  } else if (isRejected) {
    statusMessage = `After careful review, we regret to inform you that your ${safeLabel} has not been approved at this time. Please review the details below for more information.`;
    nextSteps = `
      <div style="background:#fef2f2;border:2px solid #ef4444;padding:20px;margin:24px 0;">
        <p style="margin:0 0 12px;font-weight:700;color:#7f1d1d;font-size:15px;">IMPORTANT INFORMATION</p>
        <p style="margin:0;color:#7f1d1d;font-size:14px;line-height:1.6;">If you have questions or wish to reapply, please log in to your portal or contact our support team for assistance.</p>
      </div>`;
  } else if (isInReview) {
    statusMessage = `Your ${safeLabel} is now being reviewed by our team. We will notify you once the review is complete.`;
    nextSteps = `
      <div style="background:#eff6ff;border:2px solid #3b82f6;padding:20px;margin:24px 0;">
        <p style="margin:0 0 12px;font-weight:700;color:#1e3a8a;font-size:15px;">WHAT'S NEXT</p>
        <p style="margin:0;color:#1e3a8a;font-size:14px;line-height:1.6;">Our team is currently reviewing your application. You can track the status at any time by logging in to your portal.</p>
      </div>`;
  } else {
    statusMessage = `Your ${safeLabel} status has been updated to ${safeStatus}.`;
  }

  // Notes sections
  let notesSection = '';
  if (adminNotes) {
    notesSection += `
      <div style="background:#f9fafb;border:1px solid #d1d5db;padding:16px;margin:16px 0;">
        <p style="margin:0 0 8px;font-weight:700;color:#374151;font-size:14px;">Administrator Notes:</p>
        <p style="margin:0;color:#4b5563;font-size:14px;line-height:1.6;white-space:pre-wrap;">${escapeHtml(adminNotes)}</p>
      </div>`;
  }
  if (needsActionNote) {
    notesSection += `
      <div style="background:#fffbeb;border:1px solid #f59e0b;padding:16px;margin:16px 0;">
        <p style="margin:0 0 8px;font-weight:700;color:#78350f;font-size:14px;">Action Required Details:</p>
        <p style="margin:0;color:#78350f;font-size:14px;line-height:1.6;white-space:pre-wrap;">${escapeHtml(needsActionNote)}</p>
      </div>`;
  }
  if (rejectionReason) {
    notesSection += `
      <div style="background:#fef2f2;border:1px solid #ef4444;padding:16px;margin:16px 0;">
        <p style="margin:0 0 8px;font-weight:700;color:#7f1d1d;font-size:14px;">Reason for Status:</p>
        <p style="margin:0;color:#7f1d1d;font-size:14px;line-height:1.6;white-space:pre-wrap;">${escapeHtml(rejectionReason)}</p>
      </div>`;
  }

  const content = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;background:#f3f4f6;">
  <div style="max-width:600px;margin:0 auto;background:#ffffff;">
    <!-- Header -->
    <div style="background:linear-gradient(135deg,#1e40af 0%,#3b82f6 100%);padding:32px 24px;text-align:center;">
      <h1 style="margin:0;color:#ffffff;font-size:24px;font-weight:700;">Application Status Update</h1>
    </div>

    <!-- Main Content -->
    <div style="padding:32px 24px;">
      <p style="margin:0 0 24px;color:#111827;font-size:16px;line-height:1.6;">
        ${safeName ? `Dear ${safeName},` : 'Dear Applicant,'}
      </p>

      <p style="margin:0 0 24px;color:#374151;font-size:15px;line-height:1.6;">
        ${statusMessage}
      </p>

      ${nextSteps}

      <!-- Application Details -->
      <div style="background:#f9fafb;border:1px solid #e5e7eb;padding:20px;margin:24px 0;">
        <p style="margin:0 0 12px;font-weight:700;color:#111827;font-size:15px;">Application Details</p>
        <table style="width:100%;border-collapse:collapse;">
          <tr>
            <td style="padding:8px 0;color:#6b7280;font-size:14px;">Type:</td>
            <td style="padding:8px 0;color:#111827;font-size:14px;font-weight:600;">${safeLabel}</td>
          </tr>
          <tr>
            <td style="padding:8px 0;color:#6b7280;font-size:14px;">Reference:</td>
            <td style="padding:8px 0;color:#111827;font-size:14px;font-weight:600;">${safeReference}</td>
          </tr>
          <tr>
            <td style="padding:8px 0;color:#6b7280;font-size:14px;">Status:</td>
            <td style="padding:8px 0;color:#111827;font-size:14px;font-weight:600;">${safeStatus}</td>
          </tr>
        </table>
      </div>

      ${notesSection}

      <!-- CTA Button -->
      <div style="text-align:center;margin:32px 0;">
        <a href="${safeLink}" style="display:inline-block;background:#3b82f6;color:#ffffff;text-decoration:none;padding:14px 32px;border-radius:6px;font-weight:600;font-size:15px;">View Application Details</a>
      </div>

      <p style="margin:24px 0 0;color:#6b7280;font-size:14px;line-height:1.6;">
        If you have any questions or need assistance, please don't hesitate to contact our support team.
      </p>
    </div>

    <!-- Footer -->
    <div style="background:#f9fafb;padding:24px;text-align:center;border-top:1px solid #e5e7eb;">
      <p style="margin:0 0 8px;color:#6b7280;font-size:13px;">
        This is an automated notification from the Citizen Portal.
      </p>
      <p style="margin:0;color:#9ca3af;font-size:12px;">
        Please do not reply to this email.
      </p>
    </div>
  </div>
</body>
</html>`.trim();

  return { subject, content };
};

const buildPayStubEmail = ({ employeeName, periodName, link, reference }) => {
  const safeName = escapeHtml(employeeName);
  const safePeriod = escapeHtml(periodName);
  const safeLink = escapeHtml(link);
  const safeReference = escapeHtml(reference);

  const subject = periodName
    ? `New Pay Stub Available: ${periodName}`
    : 'New Pay Stub Available';

  const content = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;background:#f3f4f6;">
  <div style="max-width:600px;margin:0 auto;background:#ffffff;">
    <!-- Header -->
    <div style="background:linear-gradient(135deg,#059669 0%,#10b981 100%);padding:32px 24px;text-align:center;">
      <h1 style="margin:0;color:#ffffff;font-size:24px;font-weight:700;">New Pay Stub Available</h1>
    </div>

    <!-- Main Content -->
    <div style="padding:32px 24px;">
      <p style="margin:0 0 24px;color:#111827;font-size:16px;line-height:1.6;">
        ${safeName ? `Dear ${safeName},` : 'Dear Employee,'}
      </p>

      <p style="margin:0 0 24px;color:#374151;font-size:15px;line-height:1.6;">
        Your pay stub${safePeriod ? ` for ${safePeriod}` : ''} is now available for viewing and download in your employee portal.
      </p>

      <!-- Pay Stub Details -->
      <div style="background:#f0fdf4;border:2px solid #22c55e;padding:20px;margin:24px 0;">
        <p style="margin:0 0 12px;font-weight:700;color:#166534;font-size:15px;">Pay Stub Information</p>
        ${safePeriod ? `<p style="margin:0;color:#166534;font-size:14px;line-height:1.6;">Period: ${safePeriod}</p>` : ''}
        <p style="margin:${safePeriod ? '8px' : '0'} 0 0;color:#166534;font-size:14px;line-height:1.6;">Reference: ${safeReference}</p>
      </div>

      <!-- CTA Button -->
      <div style="text-align:center;margin:32px 0;">
        <a href="${safeLink}" style="display:inline-block;background:#10b981;color:#ffffff;text-decoration:none;padding:14px 32px;border-radius:6px;font-weight:600;font-size:15px;">View Pay Stub</a>
      </div>

      <p style="margin:24px 0 0;color:#6b7280;font-size:14px;line-height:1.6;">
        Please log in to your portal to view detailed information and download a copy for your records.
      </p>
    </div>

    <!-- Footer -->
    <div style="background:#f9fafb;padding:24px;text-align:center;border-top:1px solid #e5e7eb;">
      <p style="margin:0 0 8px;color:#6b7280;font-size:13px;">
        This is an automated notification from the Employee Portal.
      </p>
      <p style="margin:0;color:#9ca3af;font-size:12px;">
        Please do not reply to this email.
      </p>
    </div>
  </div>
</body>
</html>`.trim();

  return { subject, content };
};

export default async function main({ req, res, log, error: errLogger }) {
  const logger = (msg) => log(`[notify] ${msg}`);
  
  // DEBUG: Log function execution details
  const executionId = process.env.APPWRITE_FUNCTION_EXECUTION_ID || 'unknown';
  const timestamp = new Date().toISOString();
  logger(`=== EXECUTION START === ID: ${executionId} | Time: ${timestamp}`);

  try {
    const client = new Client()
      .setEndpoint(process.env.APPWRITE_FUNCTION_API_ENDPOINT)
      .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
      .setKey(process.env.APPWRITE_API_KEY);

    const databases = new Databases(client);
    const users = new Users(client);
    const messaging = new Messaging(client);

    const databaseId = process.env.APPWRITE_DATABASE_ID || process.env.DATABASE_ID || 'main';
    const portalBaseUrl = process.env.PORTAL_BASE_URL || '';
    const throttleMinutes = parseInt(process.env.NOTIFY_THROTTLE_MINUTES || DEFAULT_THROTTLE_MINUTES, 10);
    const throttleMs = throttleMinutes * 60 * 1000;
    const dryRun = parseBoolean(process.env.DRY_RUN);
    const enableApplications = parseBoolean(process.env.ENABLE_APPLICATION_NOTIFICATIONS ?? true);
    const enablePayStubs = parseBoolean(process.env.ENABLE_PAY_STUB_NOTIFICATIONS ?? true);

    logger(
      `Config: throttle=${throttleMinutes}m, dryRun=${dryRun}, apps=${enableApplications}, payStubs=${enablePayStubs}`
    );

    const payload = parseJson(req.body || req.bodyRaw);
    if (!payload) {
      logger('Ignored: no payload.');
      return res.json({ ok: true, ignored: 'no_payload' });
    }

    const eventName = getEventName(req, payload);
    logger(`Event: ${eventName || 'unknown'}.`);
    
    // DEBUG: Log all headers to see if there are duplicate triggers
    const allEvents = req.headers?.['x-appwrite-events'] || req.headers?.['X-Appwrite-Events'];
    if (allEvents) {
      logger(`All Events Header: ${allEvents}`);
    }
    logger(`Event from header: ${req.headers?.['x-appwrite-event'] || req.headers?.['X-Appwrite-Event'] || 'none'}`);
    logger(`Events array from payload: ${JSON.stringify(payload?.events || [])}`);

    const document = payload;
    if (!document?.$id || !document?.$collectionId || !document?.$databaseId) {
      logger('Ignored: missing document metadata.');
      return res.json({ ok: true, ignored: 'missing_metadata' });
    }

    const collectionId = document.$collectionId;
    const applicationConfig = APPLICATION_COLLECTIONS[collectionId];
    const isPayStub = collectionId === PAY_STUBS_COLLECTION_ID;

    if (!applicationConfig && !isPayStub) {
      logger(`Ignored: unrecognized collection ${collectionId}.`);
      return res.json({ ok: true, ignored: 'unrecognized_collection' });
    }

    const eventType = guessEventType(eventName, document);
    const now = new Date();

    // ============================================
    // CRITICAL FIX: Fetch fresh document FIRST
    // ============================================
    let freshDocument = document;
    try {
      freshDocument = await databases.getDocument(databaseId, collectionId, document.$id);
      logger(`Fetched fresh document for ${collectionId}/${document.$id}`);
    } catch (err) {
      errLogger(`Failed to fetch fresh document for ${collectionId}/${document.$id}: ${err.message}`);
      // Continue with webhook payload as fallback
      logger(`Continuing with webhook payload data for ${collectionId}/${document.$id}`);
    }

    // ============================================
    // ATOMIC LOCK: Prevent race conditions
    // ============================================
    // Use a processing flag to prevent concurrent executions
    const processingKey = `processing_${collectionId}_${document.$id}_${Date.now()}`;
    const isCurrentlyProcessing = freshDocument.lastNotifiedAt && 
                                   (now - parseDate(freshDocument.lastNotifiedAt)) < 5000; // 5 seconds
    
    if (isCurrentlyProcessing) {
      logger(`Ignored: document ${collectionId}/${document.$id} is currently being processed by another execution.`);
      return res.json({ ok: true, ignored: 'concurrent_execution' });
    }

    // ============================================
    // Pay stub flow
    // ============================================
    if (isPayStub) {
      if (!enablePayStubs) {
        logger(`Ignored: pay stub emails disabled for ${freshDocument.$id}.`);
        return res.json({ ok: true, ignored: 'pay_stub_disabled' });
      }

      const lastNotifiedAt = parseDate(freshDocument.lastNotifiedAt);
      if (lastNotifiedAt && now - lastNotifiedAt < throttleMs) {
        logger(`Ignored: throttled pay stub ${freshDocument.$id}.`);
        return res.json({ ok: true, ignored: 'throttled' });
      }

      const payStubFingerprint = hashPayload({
        hash: freshDocument.hash,
        generatedAt: freshDocument.generatedAt,
        netPay: freshDocument.netPay,
        period: freshDocument.payPeriodId
      });

      if (freshDocument.lastNotifiedHash && freshDocument.lastNotifiedHash === payStubFingerprint) {
        logger(`Ignored: duplicate pay stub ${freshDocument.$id}.`);
        return res.json({ ok: true, ignored: 'duplicate' });
      }

      if (dryRun) {
        logger(`Dry run: pay stub email for ${freshDocument.$id}.`);
        return res.json({ ok: true, dryRun: true, type: 'pay_stub' });
      }

      let employee;
      try {
        employee = await databases.getDocument(databaseId, EMPLOYEES_COLLECTION_ID, freshDocument.employeeId);
      } catch (err) {
        errLogger(`Pay stub ignored: missing employee for ${freshDocument.$id}.`);
        return res.json({ ok: true, ignored: 'missing_employee' });
      }

      const userId = employee?.userId;
      if (!userId) {
        logger(`Ignored: pay stub ${freshDocument.$id} missing employee userId.`);
        return res.json({ ok: true, ignored: 'missing_userId' });
      }

      let user;
      try {
        user = await users.get(userId);
      } catch (err) {
        errLogger(`Pay stub ignored: user not found for ${freshDocument.$id}.`);
        return res.json({ ok: true, ignored: 'missing_user' });
      }

      if (!user?.email) {
        logger(`Ignored: pay stub ${freshDocument.$id} user has no email.`);
        return res.json({ ok: true, ignored: 'missing_email' });
      }

      const link = buildPortalLink(portalBaseUrl, `citizen-portal/pay-stubs/${freshDocument.$id}`);
      const reference = freshDocument.$id || freshDocument.id || 'Pay stub';
      const { subject, content } = buildPayStubEmail({
        employeeName: employee.fullName || user.name || null,
        periodName: freshDocument.periodName || null,
        link,
        reference
      });

      if (dryRun) {
        logger(`Dry run: pay stub email to ${maskEmail(user.email)} (${freshDocument.$id}).`);
        return res.json({ ok: true, dryRun: true, type: 'pay_stub' });
      }

      await messaging.createEmail({
        messageId: ID.unique(),
        subject,
        content,
        users: [userId],
        html: true
      });

      await databases.updateDocument(databaseId, collectionId, freshDocument.$id, {
        lastNotifiedAt: now.toISOString(),
        lastNotifiedType: 'pay_stub',
        lastNotifiedHash: payStubFingerprint
      });

      logger(`Email sent: pay stub ${freshDocument.$id} to ${maskEmail(user.email)}.`);
      return res.json({ ok: true, sent: true, type: 'pay_stub' });
    }

    // ============================================
    // Application flow - NOW USING FRESH DOCUMENT
    // ============================================
    
    // Extract values from fresh document for all checks and email building
    const status = normalizeStatus(freshDocument.status);
    const adminNotes = normalizeText(freshDocument.adminNotes);
    const needsActionNote = normalizeText(freshDocument.needsActionNote);
    const rejectionReason = normalizeText(freshDocument.rejectionReason);

    // Check notifiable status and notes
    const hasNotifiableStatus = NOTIFIABLE_STATUSES.has(status);
    const hasNotes = Boolean(adminNotes || needsActionNote || rejectionReason);

    if (!hasNotifiableStatus && !hasNotes) {
      logger(`Ignored: no meaningful change for ${collectionId}/${freshDocument.$id}.`);
      return res.json({ ok: true, ignored: 'no_meaningful_change' });
    }

    // Check duplicate - NOW USING FRESH DATA
    const applicationFingerprint = buildApplicationFingerprint({
      status,
      adminNotes,
      needsActionNote,
      rejectionReason
    });

    if (freshDocument.lastNotifiedHash && freshDocument.lastNotifiedHash === applicationFingerprint) {
      logger(`Ignored: duplicate notification for ${collectionId}/${freshDocument.$id}.`);
      return res.json({ ok: true, ignored: 'duplicate' });
    }

    // Check throttle
    const lastNotifiedAt = parseDate(freshDocument.lastNotifiedAt);
    if (lastNotifiedAt && now - lastNotifiedAt < throttleMs) {
      logger(`Ignored: throttled notification for ${collectionId}/${freshDocument.$id}.`);
      return res.json({ ok: true, ignored: 'throttled' });
    }

    const userId = freshDocument.userId;
    if (!userId) {
      logger(`Ignored: missing userId for ${collectionId}/${freshDocument.$id}.`);
      return res.json({ ok: true, ignored: 'missing_userId' });
    }

    const fallbackEmail = normalizeText(freshDocument.userEmail || freshDocument.email);
    if (dryRun) {
      if (!fallbackEmail) {
        logger(`Ignored: dry run missing email for ${collectionId}/${freshDocument.$id}.`);
        return res.json({ ok: true, ignored: 'missing_email' });
      }
      logger(`Dry run: application email to ${maskEmail(fallbackEmail)} (${freshDocument.$id}).`);
      return res.json({ ok: true, dryRun: true, type: 'application' });
    }

    let user;
    try {
      user = await users.get(userId);
    } catch (err) {
      errLogger(`Ignored: user not found for ${collectionId}/${freshDocument.$id}.`);
      return res.json({ ok: true, ignored: 'missing_user' });
    }

    if (!user?.email) {
      logger(`Ignored: user has no email for ${collectionId}/${freshDocument.$id}.`);
      return res.json({ ok: true, ignored: 'missing_email' });
    }

    const applicationLabel = applicationConfig.label;
    const statusLabel = STATUS_LABELS[status] || titleCase(status);
    const reference = getReference(freshDocument);
    const name = getApplicantName(freshDocument) || user.name || null;
    const link = buildPortalLink(
      portalBaseUrl,
      `citizen-portal/applications/${freshDocument.$id}?source=${applicationConfig.source}`
    );

    const { subject, content } = buildApplicationEmail({
      applicationLabel,
      statusLabel,
      reference,
      adminNotes,
      needsActionNote,
      rejectionReason,
      link,
      name
    });

    const notificationType = buildNotificationType({
      status,
      adminNotes,
      needsActionNote,
      rejectionReason
    });

    await messaging.createEmail({
      messageId: ID.unique(),
      subject,
      content,
      users: [userId],
      html: true
    });

    await databases.updateDocument(databaseId, collectionId, freshDocument.$id, {
      lastNotifiedAt: now.toISOString(),
      lastNotifiedType: notificationType,
      lastNotifiedHash: applicationFingerprint
    });

    logger(
      `Email sent: ${collectionId}/${freshDocument.$id} (${notificationType}) to ${maskEmail(user.email)}.`
    );
    return res.json({ ok: true, sent: true, type: 'application' });
  } catch (err) {
    errLogger(`Unhandled error: ${err.message}`);
    return res.json({ ok: false, error: err.message }, 500);
  }
};
