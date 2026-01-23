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
  const isActionRequired = statusLabel === STATUS_LABELS.needs_action || Boolean(needsActionNote);
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

  // Status-specific messaging
  let statusMessage = '';
  let nextSteps = '';

  if (isActionRequired || needsActionNote) {
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
    statusMessage = `After careful review, we regret to inform you that your ${safeLabel} has not been approved at this time. Please review the details below for more information regarding this decision.`;
    nextSteps = `
      <div style="background:#fef2f2;border:2px solid #ef4444;padding:20px;margin:24px 0;">
        <p style="margin:0 0 12px;font-weight:700;color:#991b1b;font-size:15px;">ADDITIONAL INFORMATION</p>
        <p style="margin:0;color:#991b1b;font-size:14px;line-height:1.6;">If you believe this decision was made in error or if you have additional information to provide, you may contact our office or submit a new application after addressing the issues mentioned in the administrator notes.</p>
      </div>`;
  } else if (isInReview) {
    statusMessage = `Your ${safeLabel} is currently under review by our office. We appreciate your patience during this process.`;
    nextSteps = `
      <div style="background:#eff6ff;border:2px solid #3b82f6;padding:20px;margin:24px 0;">
        <p style="margin:0 0 12px;font-weight:700;color:#1e40af;font-size:15px;">REVIEW IN PROGRESS</p>
        <p style="margin:0;color:#1e40af;font-size:14px;line-height:1.6;">Our office is carefully reviewing your application. We will notify you once a decision has been made or if we require any additional information. Standard processing time is 5-10 business days.</p>
      </div>`;
  } else {
    statusMessage = `There is an update to your ${safeLabel}. Please review the information below for details.`;
    nextSteps = `
      <div style="background:#f5f3ff;border:2px solid #8b5cf6;padding:20px;margin:24px 0;">
        <p style="margin:0 0 12px;font-weight:700;color:#5b21b6;font-size:15px;">PLEASE NOTE</p>
        <p style="margin:0;color:#5b21b6;font-size:14px;line-height:1.6;">Check your portal regularly for updates. We will send you notifications for any important changes or required actions.</p>
      </div>`;
  }

  const noteEntries = [];
  if (needsActionNote) noteEntries.push({ label: 'ACTION REQUIRED', value: needsActionNote });
  if (rejectionReason) noteEntries.push({ label: 'REASON FOR DECISION', value: rejectionReason });
  if (adminNotes) noteEntries.push({ label: 'ADDITIONAL INFORMATION', value: adminNotes });

  const notesHtml = noteEntries.length
    ? `<div style="margin-top:24px;background:#f9fafb;padding:20px;border:1px solid #d1d5db;">
        <p style="margin:0 0 16px;font-weight:700;color:#111827;font-size:15px;">ADMINISTRATOR NOTES</p>
        ${noteEntries
      .map(
        (entry) =>
          `<div style="margin-bottom:16px;padding-bottom:16px;border-bottom:1px solid #e5e7eb;">
                <div style="font-size:12px;font-weight:700;letter-spacing:0.05em;color:#6b7280;margin-bottom:8px;">
                  ${escapeHtml(entry.label)}
                </div>
                <div style="color:#374151;font-size:14px;line-height:1.6;">${escapeHtml(entry.value)}</div>
              </div>`
      )
      .join('')}
      </div>`
    : '';

  const ctaHtml = link
    ? `<table role="presentation" style="margin:28px auto;border-collapse:collapse;" align="center">
        <tr>
          <td style="background:#0d9488;padding:14px 40px;text-align:center;border:2px solid #0d9488;">
            <a href="${safeLink}" style="color:#ffffff;text-decoration:none;font-weight:700;font-size:14px;letter-spacing:0.025em;">VIEW APPLICATION DETAILS</a>
          </td>
        </tr>
      </table>
      <p style="margin:12px 0 0;color:#6b7280;font-size:12px;text-align:center;line-height:1.5;">If the button above does not work, copy and paste this link into your browser:<br/><span style="color:#0d9488;word-break:break-all;">${safeLink}</span></p>`
    : '';

  const statusBadge = isActionRequired
    ? `<span style="display:inline-block;padding:6px 14px;background:#fef3c7;color:#92400e;border:2px solid #f59e0b;font-size:13px;font-weight:700;letter-spacing:0.025em;">ACTION REQUIRED</span>`
    : isApproved
      ? `<span style="display:inline-block;padding:6px 14px;background:#d1fae5;color:#065f46;border:2px solid #10b981;font-size:13px;font-weight:700;letter-spacing:0.025em;">APPROVED</span>`
      : isRejected
        ? `<span style="display:inline-block;padding:6px 14px;background:#fee2e2;color:#991b1b;border:2px solid #ef4444;font-size:13px;font-weight:700;letter-spacing:0.025em;">NOT APPROVED</span>`
        : isInReview
          ? `<span style="display:inline-block;padding:6px 14px;background:#dbeafe;color:#1e40af;border:2px solid #3b82f6;font-size:13px;font-weight:700;letter-spacing:0.025em;">IN REVIEW</span>`
          : `<span style="display:inline-block;padding:6px 14px;background:#e0f2fe;color:#075985;border:2px solid #0284c7;font-size:13px;font-weight:700;letter-spacing:0.025em;">${safeStatus.toUpperCase()}</span>`;

  const currentDate = new Date().toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short'
  });

  const content = `
  <div style="font-family:Arial,Helvetica,sans-serif;background:#ffffff;padding:0;margin:0;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f3f4f6;padding:40px 20px;">
      <tr>
        <td align="center">
          <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="background:#ffffff;border:2px solid #d1d5db;max-width:600px;">
            
            <!-- Header -->
            <tr>
              <td style="background:#0d9488;padding:30px 40px;text-align:center;border-bottom:4px solid #0f766e;">
                <p style="margin:0 0 8px;font-size:13px;color:#d1fae5;letter-spacing:0.1em;font-weight:700;">GOVERNMENT PORTAL NOTIFICATION</p>
                <h1 style="margin:0;font-size:24px;color:#ffffff;font-weight:700;letter-spacing:0.025em;">${isActionRequired ? 'ACTION REQUIRED' : isApproved ? 'APPLICATION APPROVED' : isRejected ? 'APPLICATION UPDATE' : 'STATUS UPDATE'}</h1>
              </td>
            </tr>

            <!-- Body -->
            <tr>
              <td style="padding:40px;">
                
                <!-- Greeting -->
                <p style="margin:0 0 8px;color:#111827;font-size:15px;font-weight:700;">Dear ${safeName ? safeName : 'Applicant'},</p>
                
                <!-- Status Message -->
                <p style="margin:0 0 28px;color:#374151;font-size:14px;line-height:1.7;">${statusMessage}</p>

                <!-- Application Details -->
                <div style="background:#f9fafb;border:2px solid #e5e7eb;padding:24px;margin-bottom:24px;">
                  <p style="margin:0 0 20px;font-size:13px;color:#6b7280;font-weight:700;letter-spacing:0.05em;">APPLICATION DETAILS</p>
                  
                  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
                    <tr>
                      <td style="padding:12px 0;color:#6b7280;font-size:14px;border-top:1px solid #e5e7eb;">Application Type:</td>
                      <td style="padding:12px 0;color:#111827;font-weight:600;text-align:right;font-size:14px;border-top:1px solid #e5e7eb;">${safeLabel}</td>
                    </tr>
                    <tr>
                      <td style="padding:12px 0;color:#6b7280;font-size:14px;border-top:1px solid #e5e7eb;">Reference Number:</td>
                      <td style="padding:12px 0;color:#111827;font-weight:700;text-align:right;font-size:14px;font-family:Courier,monospace;border-top:1px solid #e5e7eb;">${safeReference}</td>
                    </tr>
                    <tr>
                      <td style="padding:12px 0;color:#6b7280;font-size:14px;border-top:1px solid #e5e7eb;">Current Status:</td>
                      <td style="padding:12px 0;text-align:right;border-top:1px solid #e5e7eb;">${statusBadge}</td>
                    </tr>
                    <tr>
                      <td style="padding:12px 0;color:#6b7280;font-size:14px;border-top:1px solid #e5e7eb;">Last Updated:</td>
                      <td style="padding:12px 0;color:#111827;font-weight:500;text-align:right;font-size:13px;border-top:1px solid #e5e7eb;">${currentDate}</td>
                    </tr>
                  </table>
                </div>

                <!-- Next Steps -->
                ${nextSteps}

                <!-- Admin Notes -->
                ${notesHtml}

                <!-- CTA Button -->
                ${ctaHtml}

                <!-- Help Section -->
                <div style="margin-top:36px;padding-top:28px;border-top:2px solid #e5e7eb;">
                  <p style="margin:0 0 12px;font-weight:700;color:#111827;font-size:14px;">NEED ASSISTANCE?</p>
                  <p style="margin:0 0 12px;color:#4b5563;font-size:14px;line-height:1.6;">If you have questions about your application, please contact our office using the reference number provided above. You may:</p>
                  <ul style="margin:0;padding-left:20px;color:#4b5563;font-size:14px;line-height:1.8;">
                    <li style="margin-bottom:6px;">Visit the Help Center in your portal for frequently asked questions</li>
                    <li style="margin-bottom:6px;">Contact support with your reference number for assistance</li>
                    <li style="margin-bottom:6px;">Review your portal for additional resources and documentation</li>
                  </ul>
                </div>

                <!-- Security Notice -->
                <div style="margin-top:28px;padding:18px;background:#fffbeb;border-left:4px solid #f59e0b;">
                  <p style="margin:0;color:#78350f;font-size:12px;line-height:1.6;">
                    <strong>SECURITY NOTICE:</strong> This email contains information about your government application. If you did not submit this application or believe you received this email in error, please contact our office immediately using the official contact information on our website.
                  </p>
                </div>

              </td>
            </tr>

            <!-- Footer -->
            <tr>
              <td style="background:#f9fafb;padding:24px 40px;text-align:center;border-top:2px solid #e5e7eb;">
                <p style="margin:0 0 8px;color:#6b7280;font-size:12px;line-height:1.5;">
                  This is an automated notification from the Government Portal.<br/>
                  Please do not reply directly to this email.
                </p>
                <p style="margin:0;color:#9ca3af;font-size:11px;">
                  Copyright &copy; ${new Date().getFullYear()} Government Portal. All rights reserved.
                </p>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>
  </div>`;

  return { subject, content, html: true };
};

const buildPayStubEmail = ({ employeeName, periodName, link, reference }) => {
  const subject = `Pay Stub Available - Reference ${reference}`;
  const safeName = escapeHtml(employeeName);
  const safePeriod = escapeHtml(periodName);
  const safeReference = escapeHtml(reference);
  const safeLink = escapeHtml(link);

  const ctaHtml = link
    ? `<table role="presentation" style="margin:28px auto;border-collapse:collapse;" align="center">
        <tr>
          <td style="background:#0d9488;padding:14px 40px;text-align:center;border:2px solid #0d9488;">
            <a href="${safeLink}" style="color:#ffffff;text-decoration:none;font-weight:700;font-size:14px;letter-spacing:0.025em;">VIEW PAY STUB</a>
          </td>
        </tr>
      </table>
      <p style="margin:12px 0 0;color:#6b7280;font-size:12px;text-align:center;line-height:1.5;">If the button above does not work, copy and paste this link into your browser:<br/><span style="color:#0d9488;word-break:break-all;">${safeLink}</span></p>`
    : '';

  const currentDate = new Date().toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short'
  });

  const content = `
  <div style="font-family:Arial,Helvetica,sans-serif;background:#ffffff;padding:0;margin:0;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f3f4f6;padding:40px 20px;">
      <tr>
        <td align="center">
          <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="background:#ffffff;border:2px solid #d1d5db;max-width:600px;">
            
            <!-- Header -->
            <tr>
              <td style="background:#0d9488;padding:30px 40px;text-align:center;border-bottom:4px solid #0f766e;">
                <p style="margin:0 0 8px;font-size:13px;color:#d1fae5;letter-spacing:0.1em;font-weight:700;">GOVERNMENT PORTAL NOTIFICATION</p>
                <h1 style="margin:0;font-size:24px;color:#ffffff;font-weight:700;letter-spacing:0.025em;">PAY STUB AVAILABLE</h1>
              </td>
            </tr>

            <!-- Body -->
            <tr>
              <td style="padding:40px;">
                
                <!-- Greeting -->
                <p style="margin:0 0 8px;color:#111827;font-size:15px;font-weight:700;">Dear ${safeName ? safeName : 'Employee'},</p>
                
                <!-- Main Message -->
                <p style="margin:0 0 28px;color:#374151;font-size:14px;line-height:1.7;">Your pay stub is now available for viewing and download through your employee portal. You may access it at any time using the link provided below.</p>

                <!-- Pay Stub Details -->
                <div style="background:#f9fafb;border:2px solid #e5e7eb;padding:24px;margin-bottom:24px;">
                  <p style="margin:0 0 20px;font-size:13px;color:#6b7280;font-weight:700;letter-spacing:0.05em;">PAY STUB INFORMATION</p>
                  
                  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
                    ${periodName ? `<tr>
                      <td style="padding:12px 0;color:#6b7280;font-size:14px;border-top:1px solid #e5e7eb;">Pay Period:</td>
                      <td style="padding:12px 0;color:#111827;font-weight:600;text-align:right;font-size:14px;border-top:1px solid #e5e7eb;">${safePeriod}</td>
                    </tr>` : ''}
                    <tr>
                      <td style="padding:12px 0;color:#6b7280;font-size:14px;${periodName ? 'border-top:1px solid #e5e7eb;' : ''}">Reference Number:</td>
                      <td style="padding:12px 0;color:#111827;font-weight:700;text-align:right;font-size:14px;font-family:Courier,monospace;${periodName ? 'border-top:1px solid #e5e7eb;' : ''}">${safeReference}</td>
                    </tr>
                    <tr>
                      <td style="padding:12px 0;color:#6b7280;font-size:14px;border-top:1px solid #e5e7eb;">Available Since:</td>
                      <td style="padding:12px 0;color:#111827;font-weight:500;text-align:right;font-size:13px;border-top:1px solid #e5e7eb;">${currentDate}</td>
                    </tr>
                  </table>
                </div>

                <!-- Information Box -->
                <div style="background:#eff6ff;border:2px solid #3b82f6;padding:20px;margin:24px 0;">
                  <p style="margin:0 0 12px;font-weight:700;color:#1e40af;font-size:15px;">AVAILABLE ACTIONS</p>
                  <ul style="margin:0;padding-left:20px;color:#1e40af;font-size:14px;line-height:1.8;">
                    <li style="margin-bottom:6px;">View your detailed earnings and deductions breakdown</li>
                    <li style="margin-bottom:6px;">Download a PDF copy for your personal records</li>
                    <li style="margin-bottom:6px;">Access your complete pay stub history at any time</li>
                  </ul>
                </div>

                <!-- CTA Button -->
                ${ctaHtml}

                <!-- Help Section -->
                <div style="margin-top:36px;padding-top:28px;border-top:2px solid #e5e7eb;">
                  <p style="margin:0 0 12px;font-weight:700;color:#111827;font-size:14px;">NEED ASSISTANCE?</p>
                  <p style="margin:0 0 12px;color:#4b5563;font-size:14px;line-height:1.6;">If you have questions regarding your pay stub or payroll information:</p>
                  <ul style="margin:0;padding-left:20px;color:#4b5563;font-size:14px;line-height:1.8;">
                    <li style="margin-bottom:6px;">Contact your Human Resources department or payroll administrator</li>
                    <li style="margin-bottom:6px;">Visit the Employee Help Center in your portal for frequently asked questions</li>
                    <li style="margin-bottom:6px;">Review available resources and documentation in your employee dashboard</li>
                  </ul>
                </div>

                <!-- Important Notice -->
                <div style="margin-top:28px;padding:18px;background:#fffbeb;border-left:4px solid #f59e0b;">
                  <p style="margin:0;color:#78350f;font-size:12px;line-height:1.6;">
                    <strong>IMPORTANT RECORD KEEPING:</strong> Please retain copies of your pay stubs for your personal records. Pay stubs may be required for tax filing purposes, loan applications, or other official documentation. It is recommended to keep at least twelve months of pay stub records.
                  </p>
                </div>

              </td>
            </tr>

            <!-- Footer -->
            <tr>
              <td style="background:#f9fafb;padding:24px 40px;text-align:center;border-top:2px solid #e5e7eb;">
                <p style="margin:0 0 8px;color:#6b7280;font-size:12px;line-height:1.5;">
                  This is an automated notification from the Government Portal.<br/>
                  Please do not reply directly to this email.
                </p>
                <p style="margin:0;color:#9ca3af;font-size:11px;">
                  Copyright &copy; ${new Date().getFullYear()} Government Portal. All rights reserved.
                </p>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>
  </div>`;

  return { subject, content, html: true };
};

const createClient = () => {
  const endpoint = process.env.APPWRITE_ENDPOINT;
  const project = process.env.APPWRITE_PROJECT_ID;
  const apiKey = process.env.APPWRITE_API_KEY;

  if (!endpoint || !project || !apiKey) {
    throw new Error('Missing Appwrite configuration (APPWRITE_ENDPOINT, APPWRITE_PROJECT_ID, APPWRITE_API_KEY).');
  }

  const client = new Client();
  client.setEndpoint(endpoint).setProject(project).setKey(apiKey);
  return client;
};

export default async ({ req, res, log, error }) => {
  const logger = log || console.log;
  const errLogger = error || console.error;
  const payload = parseJson(req.body) || {};
  const document = payload?.payload && payload.payload.$id ? payload.payload : payload;
  const eventName = getEventName(req, payload);
  const eventType = guessEventType(eventName, document);

  if (!document || !document.$id) {
    logger('Ignored: missing document payload.');
    return res.json({ ok: true, ignored: 'missing_document' });
  }

  const collectionId = document.$collectionId || document.collectionId;
  const databaseId =
    document.$databaseId ||
    process.env.APPWRITE_DATABASE_ID ||
    process.env.DATABASE_ID ||
    payload.$databaseId ||
    payload.databaseId;

  if (!collectionId || !databaseId) {
    logger('Ignored: missing collection or database id.');
    return res.json({ ok: true, ignored: 'missing_collection_or_database' });
  }

  const isPayStub = collectionId === PAY_STUBS_COLLECTION_ID;
  const applicationConfig = APPLICATION_COLLECTIONS[collectionId];

  if (!isPayStub && !applicationConfig) {
    logger(`Ignored: collection ${collectionId} not configured for notifications.`);
    return res.json({ ok: true, ignored: 'collection_not_supported' });
  }

  if (eventType === 'delete') {
    logger(`Ignored: delete event for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'delete_event' });
  }

  if (!isPayStub && eventType !== 'update') {
    logger(`Ignored: ${eventType} event for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'non_update_event' });
  }

  const portalBaseUrl = process.env.PORTAL_BASE_URL;
  if (!portalBaseUrl) {
    errLogger('Missing PORTAL_BASE_URL.');
    return res.json({ ok: false, error: 'missing_portal_base_url' });
  }

  const dryRun = parseBoolean(process.env.NOTIFY_DRY_RUN);
  const throttleMinutes = Number.parseInt(process.env.NOTIFY_THROTTLE_MINUTES || '', 10) || DEFAULT_THROTTLE_MINUTES;
  const throttleMs = throttleMinutes * 60 * 1000;

  let databases = null;
  let users = null;
  let messaging = null;

  if (!dryRun) {
    let client;
    try {
      client = createClient();
    } catch (err) {
      errLogger(err.message || err);
      return res.json({ ok: false, error: 'missing_appwrite_config' });
    }

    databases = new Databases(client);
    users = new Users(client);
    messaging = new Messaging(client);
  }

  const now = new Date();

  if (isPayStub) {
    const enablePayStubs = parseBoolean(process.env.ENABLE_PAYSTUB_EMAILS);
    if (!enablePayStubs) {
      logger(`Ignored: pay stub emails disabled for ${document.$id}.`);
      return res.json({ ok: true, ignored: 'pay_stub_disabled' });
    }

    const lastNotifiedAt = parseDate(document.lastNotifiedAt);
    if (lastNotifiedAt && now - lastNotifiedAt < throttleMs) {
      logger(`Ignored: throttled pay stub ${document.$id}.`);
      return res.json({ ok: true, ignored: 'throttled' });
    }

    const payStubFingerprint = hashPayload({
      hash: document.hash,
      generatedAt: document.generatedAt,
      netPay: document.netPay,
      period: document.payPeriodId
    });

    if (document.lastNotifiedHash && document.lastNotifiedHash === payStubFingerprint) {
      logger(`Ignored: duplicate pay stub ${document.$id}.`);
      return res.json({ ok: true, ignored: 'duplicate' });
    }

    if (dryRun) {
      logger(`Dry run: pay stub email for ${document.$id}.`);
      return res.json({ ok: true, dryRun: true, type: 'pay_stub' });
    }

    let employee;
    try {
      employee = await databases.getDocument(databaseId, EMPLOYEES_COLLECTION_ID, document.employeeId);
    } catch (err) {
      errLogger(`Pay stub ignored: missing employee for ${document.$id}.`);
      return res.json({ ok: true, ignored: 'missing_employee' });
    }

    const userId = employee?.userId;
    if (!userId) {
      logger(`Ignored: pay stub ${document.$id} missing employee userId.`);
      return res.json({ ok: true, ignored: 'missing_userId' });
    }

    let user;
    try {
      user = await users.get(userId);
    } catch (err) {
      errLogger(`Pay stub ignored: user not found for ${document.$id}.`);
      return res.json({ ok: true, ignored: 'missing_user' });
    }

    if (!user?.email) {
      logger(`Ignored: pay stub ${document.$id} user has no email.`);
      return res.json({ ok: true, ignored: 'missing_email' });
    }

    const link = buildPortalLink(portalBaseUrl, `citizen-portal/pay-stubs/${document.$id}`);
    const reference = document.$id || document.id || 'Pay stub';
    const { subject, content, html } = buildPayStubEmail({
      employeeName: employee.fullName || user.name || null,
      periodName: document.periodName || null,
      link,
      reference
    });

    if (dryRun) {
      logger(`Dry run: pay stub email to ${maskEmail(user.email)} (${document.$id}).`);
      return res.json({ ok: true, dryRun: true, type: 'pay_stub' });
    }

    await messaging.createEmail({
      messageId: ID.unique(),
      subject,
      content,
      users: [userId],
      html
    });

    await databases.updateDocument(databaseId, collectionId, document.$id, {
      lastNotifiedAt: now.toISOString(),
      lastNotifiedType: 'pay_stub',
      lastNotifiedHash: payStubFingerprint
    });

    logger(`Email sent: pay stub ${document.$id} to ${maskEmail(user.email)}.`);
    return res.json({ ok: true, sent: true, type: 'pay_stub' });
  }

  // Check notifiable status and notes with current document state
  const hasNotifiableStatus = NOTIFIABLE_STATUSES.has(normalizeStatus(document.status));
  const hasNotes = Boolean(
    normalizeText(document.adminNotes) ||
    normalizeText(document.needsActionNote) ||
    normalizeText(document.rejectionReason)
  );

  if (!hasNotifiableStatus && !hasNotes) {
    logger(`Ignored: no meaningful change for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'no_meaningful_change' });
  }

  // Check duplicate with current document state
  const applicationFingerprint = buildApplicationFingerprint({
    status: normalizeStatus(document.status),
    adminNotes: normalizeText(document.adminNotes),
    needsActionNote: normalizeText(document.needsActionNote),
    rejectionReason: normalizeText(document.rejectionReason)
  });

  if (document.lastNotifiedHash && document.lastNotifiedHash === applicationFingerprint) {
    logger(`Ignored: duplicate notification for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'duplicate' });
  }

  // Check throttle
  const lastNotifiedAt = parseDate(document.lastNotifiedAt);
  if (lastNotifiedAt && now - lastNotifiedAt < throttleMs) {
    logger(`Ignored: throttled notification for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'throttled' });
  }

  // Fetch fresh document to get the latest status and notes
  if (!dryRun) {
    try {
      const freshDocument = await databases.getDocument(databaseId, collectionId, document.$id);
      // Update our working document with fresh data
      document = { ...document, ...freshDocument };
    } catch (err) {
      errLogger(`Failed to fetch fresh document for ${collectionId}/${document.$id}: ${err.message}`);
      return res.json({ ok: false, error: 'fetch_failed' });
    }
  }

  // NOW extract the values from the fresh document for email building
  const status = normalizeStatus(document.status);
  const adminNotes = normalizeText(document.adminNotes);
  const needsActionNote = normalizeText(document.needsActionNote);
  const rejectionReason = normalizeText(document.rejectionReason);

  const userId = document.userId;
  if (!userId) {
    logger(`Ignored: missing userId for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'missing_userId' });
  }

  const fallbackEmail = normalizeText(document.userEmail || document.email);
  if (dryRun) {
    if (!fallbackEmail) {
      logger(`Ignored: dry run missing email for ${collectionId}/${document.$id}.`);
      return res.json({ ok: true, ignored: 'missing_email' });
    }
    logger(`Dry run: application email to ${maskEmail(fallbackEmail)} (${document.$id}).`);
    return res.json({ ok: true, dryRun: true, type: 'application' });
  }

  let user;
  try {
    user = await users.get(userId);
  } catch (err) {
    errLogger(`Ignored: user not found for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'missing_user' });
  }

  if (!user?.email) {
    logger(`Ignored: user has no email for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'missing_email' });
  }

  const applicationLabel = applicationConfig.label;
  const statusLabel = STATUS_LABELS[status] || titleCase(status);
  const reference = getReference(document);
  const name = getApplicantName(document) || user.name || null;
  const link = buildPortalLink(
    portalBaseUrl,
    `citizen-portal/applications/${document.$id}?source=${applicationConfig.source}`
  );

  const { subject, content, html } = buildApplicationEmail({
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
    html
  });

  // Recalculate fingerprint with fresh data for updating the document
  const freshApplicationFingerprint = buildApplicationFingerprint({
    status,
    adminNotes,
    needsActionNote,
    rejectionReason
  });

  await databases.updateDocument(databaseId, collectionId, document.$id, {
    lastNotifiedAt: now.toISOString(),
    lastNotifiedType: notificationType,
    lastNotifiedHash: freshApplicationFingerprint
  });

  logger(
    `Email sent: ${collectionId}/${document.$id} (${notificationType}) to ${maskEmail(user.email)}.`
  );
  return res.json({ ok: true, sent: true, type: 'application' });
};