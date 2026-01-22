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
  const subject = isActionRequired
    ? `Action Required: ${applicationLabel}`
    : `Update: Your ${applicationLabel} is now ${statusLabel}`;

  const lines = [];
  lines.push(`Hello${name ? ` ${name}` : ''},`);
  lines.push('');
  lines.push(`There is an update to your ${applicationLabel}.`);
  lines.push(`Reference ID: ${reference}`);
  lines.push(`Status: ${statusLabel}`);

  const noteEntries = [];
  if (needsActionNote) noteEntries.push({ label: 'Action required', value: needsActionNote });
  if (rejectionReason) noteEntries.push({ label: 'Rejection reason', value: rejectionReason });
  if (adminNotes) noteEntries.push({ label: 'Admin note', value: adminNotes });

  if (noteEntries.length) {
    lines.push('');
    noteEntries.forEach((entry) => {
      lines.push(`${entry.label}: ${entry.value}`);
    });
  }

  if (link) {
    lines.push('');
    lines.push(`View your application: ${link}`);
  }

  lines.push('');
  lines.push('Thank you,');
  lines.push('Government Portal');

  return { subject, content: lines.join('\n') };
};

const buildPayStubEmail = ({ employeeName, periodName, link, reference }) => {
  const subject = 'Update: Your pay stub is ready';
  const lines = [];
  lines.push(`Hello${employeeName ? ` ${employeeName}` : ''},`);
  lines.push('');
  lines.push('Your pay stub is now available.');
  if (periodName) {
    lines.push(`Pay period: ${periodName}`);
  }
  lines.push(`Reference ID: ${reference}`);
  if (link) {
    lines.push('');
    lines.push(`View your pay stub: ${link}`);
  }
  lines.push('');
  lines.push('Thank you,');
  lines.push('Government Portal');

  return { subject, content: lines.join('\n') };
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
    const { subject, content } = buildPayStubEmail({
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
      html: false
    });

    await databases.updateDocument(databaseId, collectionId, document.$id, {
      lastNotifiedAt: now.toISOString(),
      lastNotifiedType: 'pay_stub',
      lastNotifiedHash: payStubFingerprint
    });

    logger(`Email sent: pay stub ${document.$id} to ${maskEmail(user.email)}.`);
    return res.json({ ok: true, sent: true, type: 'pay_stub' });
  }

  const status = normalizeStatus(document.status);
  const adminNotes = normalizeText(document.adminNotes);
  const needsActionNote = normalizeText(document.needsActionNote);
  const rejectionReason = normalizeText(document.rejectionReason);

  const hasNotifiableStatus = NOTIFIABLE_STATUSES.has(status);
  const hasNotes = Boolean(adminNotes || needsActionNote || rejectionReason);

  if (!hasNotifiableStatus && !hasNotes) {
    logger(`Ignored: no meaningful change for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'no_meaningful_change' });
  }

  const applicationFingerprint = buildApplicationFingerprint({
    status,
    adminNotes,
    needsActionNote,
    rejectionReason
  });

  if (document.lastNotifiedHash && document.lastNotifiedHash === applicationFingerprint) {
    logger(`Ignored: duplicate notification for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'duplicate' });
  }

  const lastNotifiedAt = parseDate(document.lastNotifiedAt);
  if (lastNotifiedAt && now - lastNotifiedAt < throttleMs) {
    logger(`Ignored: throttled notification for ${collectionId}/${document.$id}.`);
    return res.json({ ok: true, ignored: 'throttled' });
  }

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
    html: false
  });

  await databases.updateDocument(databaseId, collectionId, document.$id, {
    lastNotifiedAt: now.toISOString(),
    lastNotifiedType: notificationType,
    lastNotifiedHash: applicationFingerprint
  });

  logger(
    `Email sent: ${collectionId}/${document.$id} (${notificationType}) to ${maskEmail(user.email)}.`
  );
  return res.json({ ok: true, sent: true, type: 'application' });
};
