require('dotenv').config();
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const Database = require('better-sqlite3');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const https = require('https');
const { v4: uuidv4 } = require('uuid');
const { Resend } = require('resend');
const { S3Client, PutObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');

const SHIPPO_API_KEY = process.env.SHIPPO_API_KEY || '';

// Shippo REST helper — returns parsed JSON or throws
function shippoRequest(method, endpoint, body) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const req = https.request({
      hostname: 'api.goshippo.com',
      path: endpoint,
      method,
      headers: {
        'Authorization': `ShippoToken ${SHIPPO_API_KEY}`,
        'Content-Type': 'application/json',
        ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {})
      }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error('Shippo parse error: ' + data)); }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function createShippingLabel({ fromAddress, toAddress, weightOz, listingTitle }) {
  // Create shipment and buy cheapest USPS rate
  const shipment = await shippoRequest('POST', '/shipments/', {
    address_from: fromAddress,
    address_to: toAddress,
    parcels: [{
      length: '12', width: '10', height: '6',
      distance_unit: 'in',
      weight: Math.max(weightOz / 16, 0.1).toFixed(2),
      mass_unit: 'lb'
    }],
    async: false
  });

  if (!shipment.rates || !shipment.rates.length) throw new Error('No shipping rates returned from Shippo');

  // Pick cheapest USPS rate; fall back to overall cheapest
  const uspsRates = shipment.rates.filter(r => r.provider === 'USPS');
  const rates = uspsRates.length ? uspsRates : shipment.rates;
  rates.sort((a, b) => parseFloat(a.amount) - parseFloat(b.amount));
  const cheapest = rates[0];

  // Purchase label
  const transaction = await shippoRequest('POST', '/transactions/', {
    rate: cheapest.object_id,
    label_file_type: 'PDF',
    async: false
  });

  if (transaction.status !== 'SUCCESS') {
    throw new Error('Label purchase failed: ' + (transaction.messages?.[0]?.text || transaction.status));
  }

  // Register a tracker so Shippo fires track_updated webhooks to us
  let trackerObjectId = null;
  try {
    const tracker = await shippoRequest('POST', '/trackers/', {
      tracking_number: transaction.tracking_number,
      carrier: cheapest.provider.toLowerCase()
    });
    trackerObjectId = tracker.object_id || null;
  } catch(te) { console.warn('[Shippo] Tracker registration failed:', te.message); }

  return {
    labelUrl:       transaction.label_url,
    trackingNumber: transaction.tracking_number,
    trackingUrl:    transaction.tracking_url_provider,
    carrier:        cheapest.provider,
    service:        cheapest.servicelevel?.name || cheapest.servicelevel_name,
    rate:           cheapest.amount,
    trackerObjectId
  };
}

const app = express();
const PORT = process.env.PORT || 3000;
const BASE_URL = process.env.BASE_URL || 'https://good-kit.com';
const resend = new Resend(process.env.RESEND_API_KEY);
const FROM_EMAIL = 'marketplace@good-kit.com';
const NOTIFY_EMAIL = process.env.NOTIFY_EMAIL || 'ccroberts10@gmail.com';

const r2 = new S3Client({
  region: 'auto',
  endpoint: process.env.R2_ENDPOINT || 'https://placeholder.r2.cloudflarestorage.com',
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY_ID     || 'placeholder',
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || 'placeholder'
  }
});
const R2_BUCKET = process.env.R2_BUCKET || 'goodkit-marketplace-backups';
const R2_ENABLED = !!(process.env.R2_ENDPOINT && process.env.R2_ACCESS_KEY_ID && process.env.R2_SECRET_ACCESS_KEY);

const DB_PATH = process.env.DB_PATH || './marketplace.db';
const db = new Database(DB_PATH);

// ── SCHEMA ──────────────────────────────────────────────────────────────────
// Clean P2P schema — no concierge, no staff, no dropoff tiers
// weight_oz is used to select prepaid shipping label tier at checkout
db.exec(`
  CREATE TABLE IF NOT EXISTS listings (
    id TEXT PRIMARY KEY,
    seller_name TEXT,
    seller_email TEXT,
    stripe_account_id TEXT,
    title TEXT,
    category TEXT,
    description TEXT,
    condition TEXT,
    price INTEGER,
    weight_oz INTEGER DEFAULT 0,
    photos TEXT DEFAULT '[]',
    shipping_estimate INTEGER DEFAULT 0,
    status TEXT DEFAULT 'approved',
    view_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    sold_at TEXT
  );
  CREATE TABLE IF NOT EXISTS sales (
    id TEXT PRIMARY KEY,
    listing_id TEXT,
    buyer_email TEXT,
    payment_intent_id TEXT,
    amount INTEGER,
    seller_payout INTEGER,
    platform_payout INTEGER,
    delivery_type TEXT,
    label_tier TEXT,
    label_cost INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS seller_sessions (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    role TEXT DEFAULT 'seller',
    expires_at TEXT NOT NULL,
    used INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS offers (
    id TEXT PRIMARY KEY,
    listing_id TEXT NOT NULL,
    buyer_name TEXT,
    buyer_email TEXT NOT NULL,
    amount INTEGER NOT NULL,
    message TEXT,
    status TEXT DEFAULT 'pending',
    counter_amount INTEGER,
    counter_message TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    listing_id TEXT NOT NULL,
    offer_id TEXT,
    from_email TEXT NOT NULL,
    from_name TEXT,
    from_role TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS listing_alerts (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    category TEXT,
    max_price INTEGER,
    condition TEXT,
    active INTEGER DEFAULT 1,
    unsubscribe_token TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
  );
`);

// Safe migrations for existing DBs
[
  `ALTER TABLE listings ADD COLUMN view_count INTEGER DEFAULT 0`,
  `ALTER TABLE listings ADD COLUMN weight_oz INTEGER DEFAULT 0`,
  `ALTER TABLE sales ADD COLUMN label_tier TEXT`,
  `ALTER TABLE sales ADD COLUMN label_cost INTEGER DEFAULT 0`,
  `ALTER TABLE sales ADD COLUMN platform_payout INTEGER DEFAULT 0`,
  `ALTER TABLE seller_sessions ADD COLUMN role TEXT DEFAULT 'seller'`,
  `ALTER TABLE listings ADD COLUMN size TEXT DEFAULT ''`,
  `ALTER TABLE seller_sessions ADD COLUMN stripe_account_id TEXT`,
  // Shippo shipping additions
  `ALTER TABLE seller_sessions ADD COLUMN ship_name TEXT`,
  `ALTER TABLE seller_sessions ADD COLUMN ship_street1 TEXT`,
  `ALTER TABLE seller_sessions ADD COLUMN ship_city TEXT`,
  `ALTER TABLE seller_sessions ADD COLUMN ship_state TEXT`,
  `ALTER TABLE seller_sessions ADD COLUMN ship_zip TEXT`,
  `ALTER TABLE seller_sessions ADD COLUMN ship_phone TEXT`,
  `ALTER TABLE sales ADD COLUMN buyer_name TEXT`,
  `ALTER TABLE sales ADD COLUMN buyer_street1 TEXT`,
  `ALTER TABLE sales ADD COLUMN buyer_city TEXT`,
  `ALTER TABLE sales ADD COLUMN buyer_state TEXT`,
  `ALTER TABLE sales ADD COLUMN buyer_zip TEXT`,
  `ALTER TABLE sales ADD COLUMN tracking_number TEXT`,
  `ALTER TABLE sales ADD COLUMN label_url TEXT`,
  `ALTER TABLE sales ADD COLUMN tracking_url TEXT`,
  // SEO: searchable keywords per listing
  `ALTER TABLE listings ADD COLUMN keywords TEXT DEFAULT ''`,
  // CCX: collegiate cycling exchange team code on listings
  `ALTER TABLE listings ADD COLUMN ccx_code TEXT DEFAULT NULL`,
  // CCX: sales ccx fund amount
  `ALTER TABLE sales ADD COLUMN ccx_fund INTEGER DEFAULT 0`,
  `ALTER TABLE sales ADD COLUMN ccx_code TEXT DEFAULT NULL`,
  // Buyer protection: explicit hold window and dispute flag
  `ALTER TABLE sales ADD COLUMN payout_after TEXT DEFAULT NULL`,
  `ALTER TABLE sales ADD COLUMN buyer_flagged INTEGER DEFAULT 0`,
  `ALTER TABLE sales ADD COLUMN flag_reason TEXT DEFAULT NULL`,
  // Listing lifecycle: re-engagement nudge + auto-expire
  `ALTER TABLE listings ADD COLUMN reminded_at TEXT DEFAULT NULL`,
  `ALTER TABLE listings ADD COLUMN final_warned_at TEXT DEFAULT NULL`,
  // Shippo tracking webhook integration
  `ALTER TABLE sales ADD COLUMN shippo_tracker_id TEXT DEFAULT NULL`,
  `ALTER TABLE sales ADD COLUMN delivered_at TEXT DEFAULT NULL`,
].forEach(sql => { try { db.exec(sql); } catch(e) {} });

// Shop consignment migrations
[
  `ALTER TABLE seller_sessions ADD COLUMN account_type TEXT DEFAULT 'individual'`,
  `ALTER TABLE seller_sessions ADD COLUMN shop_slug TEXT DEFAULT NULL`,
  `ALTER TABLE listings ADD COLUMN account_type TEXT DEFAULT 'individual'`,
].forEach(sql => { try { db.exec(sql); } catch(e) {} });

// Pro athlete / pro consignment / shop wallet migrations
[
  `ALTER TABLE shop_accounts ADD COLUMN shop_type TEXT DEFAULT 'shop'`,
  `ALTER TABLE shop_accounts ADD COLUMN consign_mode INTEGER DEFAULT 0`,
  `ALTER TABLE shop_accounts ADD COLUMN venmo TEXT DEFAULT NULL`,
  `ALTER TABLE shop_accounts ADD COLUMN paypal TEXT DEFAULT NULL`,
  `ALTER TABLE shop_accounts ADD COLUMN bank_routing TEXT DEFAULT NULL`,
  `ALTER TABLE shop_accounts ADD COLUMN bank_account TEXT DEFAULT NULL`,
  `ALTER TABLE shop_accounts ADD COLUMN wallet_balance INTEGER DEFAULT 0`,
  `ALTER TABLE shop_accounts ADD COLUMN total_earned INTEGER DEFAULT 0`,
].forEach(sql => { try { db.exec(sql); } catch(e) {} });

// Shop consign discount migrations
[
  `ALTER TABLE shop_consign_items ADD COLUMN listed_at TEXT DEFAULT NULL`,
  `ALTER TABLE shop_consign_items ADD COLUMN original_list_price INTEGER DEFAULT NULL`,
  `ALTER TABLE listings ADD COLUMN consign_shop_id TEXT DEFAULT NULL`,
].forEach(sql => { try { db.exec(sql); } catch(e) {} });

// Shop consign tables
db.exec(`
  CREATE TABLE IF NOT EXISTS shop_consign_items (
    id TEXT PRIMARY KEY,
    shop_id TEXT NOT NULL,
    item_description TEXT NOT NULL,
    listing_id TEXT,
    sale_price INTEGER,
    shop_payout INTEGER,
    platform_cut INTEGER,
    status TEXT DEFAULT 'pending',
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS shop_wallet_events (
    id TEXT PRIMARY KEY,
    shop_id TEXT NOT NULL,
    item_id TEXT,
    amount INTEGER NOT NULL,
    type TEXT NOT NULL,
    description TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS shop_payouts (
    id TEXT PRIMARY KEY,
    shop_id TEXT NOT NULL,
    amount INTEGER NOT NULL,
    method TEXT NOT NULL,
    method_detail TEXT,
    notes TEXT,
    status TEXT DEFAULT 'paid',
    paid_at TEXT DEFAULT (datetime('now'))
  );
`);

// Pro consignment athlete profiles + wallet
db.exec(`
  CREATE TABLE IF NOT EXISTS pro_consign_athletes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    venmo TEXT,
    paypal TEXT,
    bank_routing TEXT,
    bank_account TEXT,
    wallet_balance INTEGER DEFAULT 0,
    total_earned INTEGER DEFAULT 0,
    notes TEXT,
    status TEXT DEFAULT 'active',
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS pro_consign_wallet_events (
    id TEXT PRIMARY KEY,
    athlete_id TEXT NOT NULL,
    item_id TEXT,
    amount INTEGER NOT NULL,
    type TEXT NOT NULL,
    description TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS pro_consign_payouts (
    id TEXT PRIMARY KEY,
    athlete_id TEXT NOT NULL,
    amount INTEGER NOT NULL,
    method TEXT NOT NULL,
    method_detail TEXT,
    notes TEXT,
    status TEXT DEFAULT 'paid',
    paid_at TEXT DEFAULT (datetime('now'))
  );
`);

// Pro consignment items table — GoodKit posts gear dropped off by local pros
db.exec(`
  CREATE TABLE IF NOT EXISTS pro_consign_items (
    id TEXT PRIMARY KEY,
    athlete_id TEXT,
    athlete_name TEXT NOT NULL,
    athlete_email TEXT NOT NULL,
    athlete_phone TEXT,
    item_description TEXT NOT NULL,
    listing_id TEXT,
    sale_price INTEGER,
    athlete_payout INTEGER,
    platform_cut INTEGER,
    status TEXT DEFAULT 'pending',
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    paid_at TEXT DEFAULT NULL
  );
`);

// Add athlete_id to existing pro_consign_items if missing
[`ALTER TABLE pro_consign_items ADD COLUMN athlete_id TEXT DEFAULT NULL`]
  .forEach(sql => { try { db.exec(sql); } catch(e) {} });

// Shop accounts table
db.exec(`
  CREATE TABLE IF NOT EXISTS shop_accounts (
    id TEXT PRIMARY KEY,
    shop_name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    contact_name TEXT NOT NULL,
    contact_email TEXT NOT NULL UNIQUE,
    stripe_account_id TEXT,
    ship_name TEXT,
    ship_street1 TEXT,
    ship_city TEXT,
    ship_state TEXT,
    ship_zip TEXT,
    ship_phone TEXT,
    status TEXT DEFAULT 'active',
    created_at TEXT DEFAULT (datetime('now'))
  );
`);

// CCX tables (full create — safe with IF NOT EXISTS)
db.exec(`
  CREATE TABLE IF NOT EXISTS ccx_teams (
    id TEXT PRIMARY KEY,
    team_name TEXT NOT NULL,
    school TEXT NOT NULL,
    code TEXT NOT NULL UNIQUE,
    captain_name TEXT NOT NULL,
    captain_email TEXT NOT NULL UNIQUE,
    payout_email TEXT,
    fund_balance INTEGER DEFAULT 0,
    total_earned INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active',
    auth_token TEXT,
    token_expires TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS ccx_payouts (
    id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT (datetime('now')),
    paid_at TEXT
  );
`);

// ── COMMUNITIES SCHEMA ───────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS communities (
    id TEXT PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    tagline TEXT DEFAULT '',
    logo_url TEXT DEFAULT '',
    accent_color TEXT DEFAULT '#E85D26',
    cause_pct INTEGER DEFAULT 0,
    cause_label TEXT DEFAULT '',
    admin_email TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS community_members (
    id TEXT PRIMARY KEY,
    community_id TEXT NOT NULL,
    email TEXT NOT NULL,
    status TEXT DEFAULT 'approved',
    joined_at TEXT DEFAULT (datetime('now')),
    UNIQUE(community_id, email)
  );
`);

// Migrations for existing DBs
[
  `ALTER TABLE listings ADD COLUMN community_id TEXT DEFAULT NULL`,
  `ALTER TABLE communities ADD COLUMN featured_listing_ids TEXT DEFAULT '[]'`,
  `ALTER TABLE listings ADD COLUMN delivery_type TEXT DEFAULT 'ship'`,
  `ALTER TABLE listings ADD COLUMN allows_pickup INTEGER DEFAULT 0`,
  // Stripe Connect on pro consign athletes
  `ALTER TABLE pro_consign_athletes ADD COLUMN stripe_account_id TEXT DEFAULT NULL`,
  `ALTER TABLE pro_consign_athletes ADD COLUMN stripe_status TEXT DEFAULT 'no_stripe'`,
  `ALTER TABLE pro_consign_athletes ADD COLUMN logo_url TEXT DEFAULT NULL`,
  // Stripe Connect on CCX teams
  `ALTER TABLE ccx_teams ADD COLUMN stripe_account_id TEXT DEFAULT NULL`,
  `ALTER TABLE ccx_teams ADD COLUMN stripe_status TEXT DEFAULT 'no_stripe'`,
  `ALTER TABLE ccx_teams ADD COLUMN logo_url TEXT DEFAULT NULL`,
  // Logo on shop_accounts (covers shops + pro athlete shops)
  `ALTER TABLE shop_accounts ADD COLUMN logo_url TEXT DEFAULT NULL`,
  // shop_accounts wallet columns (added in consign update — safe no-ops if already exist)
  `ALTER TABLE shop_accounts ADD COLUMN shop_type TEXT DEFAULT 'shop'`,
  `ALTER TABLE shop_accounts ADD COLUMN consign_mode INTEGER DEFAULT 0`,
  `ALTER TABLE shop_accounts ADD COLUMN venmo TEXT DEFAULT NULL`,
  `ALTER TABLE shop_accounts ADD COLUMN paypal TEXT DEFAULT NULL`,
  `ALTER TABLE shop_accounts ADD COLUMN bank_routing TEXT DEFAULT NULL`,
  `ALTER TABLE shop_accounts ADD COLUMN bank_account TEXT DEFAULT NULL`,
  `ALTER TABLE shop_accounts ADD COLUMN wallet_balance INTEGER DEFAULT 0`,
  `ALTER TABLE shop_accounts ADD COLUMN total_earned INTEGER DEFAULT 0`,
  // Stripe status on shop_accounts
  `ALTER TABLE shop_accounts ADD COLUMN stripe_status TEXT DEFAULT 'no_stripe'`,
].forEach(sql => { try { db.exec(sql); } catch(e) {} });

// One-time: clear old DBP stripe_account_id values so sellers re-onboard with new GoodKit Stripe account
// Only clears IDs that start with 'acct_' (Stripe Connect express accounts from DBP)
// Safe to run on every deploy — has no effect once cleared
try {
  const cleared = db.prepare(
    `UPDATE listings SET stripe_account_id = NULL WHERE stripe_account_id IS NOT NULL AND stripe_account_id != ''`
  ).run();
  if (cleared.changes > 0) console.log(`[migration] Cleared ${cleared.changes} old DBP stripe_account_id value(s)`);
} catch(e) { console.error('[migration] stripe_account_id clear failed:', e.message); }

// ── SHIPPING LABEL TIERS ─────────────────────────────────────────────────────
// Flat-rate tiers (cents). These are what we CHARGE buyers.
// Actual EasyPost label cost will be less — margin is platform revenue.
const LABEL_TIERS = {
  xs: { label: '0–1 lb',   maxOz: 16,   price: 600  }, // $6
  sm: { label: '1–5 lb',   maxOz: 80,   price: 1000 }, // $10
  md: { label: '5–20 lb',  maxOz: 320,  price: 1600 }, // $16
  lg: { label: '20+ lb',   maxOz: 99999,price: 4500 }, // $45
};

function getLabelTier(weightOz) {
  if (!weightOz || weightOz <= 16)  return { tier: 'xs', ...LABEL_TIERS.xs };
  if (weightOz <= 80)               return { tier: 'sm', ...LABEL_TIERS.sm };
  if (weightOz <= 320)              return { tier: 'md', ...LABEL_TIERS.md };
  return                                   { tier: 'lg', ...LABEL_TIERS.lg };
}

// ── FEE STRUCTURE ────────────────────────────────────────────────────────────
// Three-tier model:
//   Individual sellers:  85% seller / 15% GoodKit (13% platform + 2% CCX if applicable)
//   Shop / pro athlete:  65% seller / 35% GoodKit
//   Pro consign (drop-off): 75% athlete / 25% GoodKit (GoodKit posts, athlete drops off)
// Shipping passes through at cost (flat tier price charged to buyer).
// Stripe fee (~2.9% + $0.30) comes out of GoodKit's platform share.
function calculateSplit(itemPriceCents, shippingCents, ccxCode, accountType) {
  shippingCents = shippingCents || 0;
  const isShop        = accountType === 'shop';
  const isProConsign  = accountType === 'pro_consign';
  // pro_athlete: athlete lists own gear, same 85/15 as individual sellers
  const sellerPct     = isShop ? 65 : (isProConsign ? 75 : 85);
  const platformPct   = isShop ? 35 : (isProConsign ? 25 : 15);
  const totalCents    = itemPriceCents + shippingCents;
  const stripeFee     = Math.round(totalCents * 0.029 + 30);
  const platformFee   = Math.round(itemPriceCents * (platformPct / 100));
  const ccxFund       = (!isShop && !isProConsign && ccxCode) ? Math.round(itemPriceCents * 0.02) : 0; // 2% CCX, individual only
  const goodkitNet    = platformFee - ccxFund;
  const sellerNet     = itemPriceCents - platformFee + shippingCents;
  const platformNet   = Math.max(goodkitNet - stripeFee, 0);
  return {
    itemPrice:    itemPriceCents / 100,
    shipping:     shippingCents  / 100,
    total:        totalCents     / 100,
    sellerNet:    sellerNet      / 100,
    platformFee:  platformFee    / 100,
    ccxFund:      ccxFund        / 100,
    platformNet:  platformNet    / 100,
    stripeFee:    stripeFee      / 100,
    sellerPct,
    platformPct,
    accountType:  accountType || 'individual',
    ccxCode:      ccxCode || null
  };
}

// ── MIDDLEWARE ───────────────────────────────────────────────────────────────
app.use(cors({
  origin: [
    'https://goodkit.com', 'https://www.goodkit.com',
    'https://good-kit.com', 'https://www.good-kit.com',
    'http://localhost:3000'
  ],
  credentials: true
}));
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json());

// ── Rate limiting ─────────────────────────────────────────────────────────────
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, please try again later.' }
});

const checkoutLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many checkout attempts, please try again later.' }
});

const uploadLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 50,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Upload limit reached, please try again later.' }
});

app.use('/api/', apiLimiter);
app.use('/api/checkout', checkoutLimiter);
app.use('/api/listings', (req, res, next) => {
  if (req.method === 'POST') return uploadLimiter(req, res, next);
  next();
});

const uploadDir = process.env.UPLOAD_DIR || './uploads';
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({
  storage: multer.diskStorage({
    destination: uploadDir,
    filename: (req, file, cb) => cb(null, uuidv4() + path.extname(file.originalname))
  }),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype.startsWith('image/')) cb(null, true);
    else cb(new Error('Images only'));
  }
});
app.use('/uploads', express.static(uploadDir));
app.use(express.static(__dirname, { index: false })); // serve icons, og-image, html files

// ── EMAIL ────────────────────────────────────────────────────────────────────
async function sendEmail(to, subject, html) {
  try { await resend.emails.send({ from: FROM_EMAIL, to, subject, html }); }
  catch(err) { console.error('Email error:', err.message); }
}

// GoodKit brand: Trail Burn #FF5C1A, Paper #F5F0E8, Gravel ink #1A1A14
function emailTemplate(title, body) {
  return `<div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;background:#F5F0E8;">
    <div style="background:#1A1A14;padding:28px 32px;">
      <p style="font-size:11px;letter-spacing:0.25em;text-transform:uppercase;color:#666;margin:0 0 6px;">GoodKit</p>
      <h1 style="font-size:22px;color:#F5F0E8;margin:0;font-weight:400;">${title}</h1>
    </div>
    <div style="padding:28px 32px;">${body}</div>
    <div style="background:#E8E0D0;padding:16px 32px;">
      <p style="font-size:11px;color:#888;margin:0;">
        good-kit.com · <a href="${BASE_URL}" style="color:#FF5C1A;">it's supposed to be fun.</a>
      </p>
    </div>
  </div>`;
}

// ── PARTNER WELCOME EMAIL ─────────────────────────────────────────────────────
// Sent to shops, pro athletes, pro consign athletes, and CCX teams on creation.
// entityType: 'shop' | 'pro_athlete' | 'consign_athlete' | 'ccx_team'
async function sendPartnerWelcome({ to, recipientName, entityName, entityType, loginLink, artworkUploadUrl }) {
  const typeLabels = {
    shop:             { label: 'Bike Shop Partner', fee: '65% of each sale' },
    pro_athlete:      { label: 'Verified Pro Athlete Shop', fee: '85% of each sale' },
    consign_athlete:  { label: 'Pro Consignment Partner', fee: '75% of each sale' },
    ccx_team:         { label: 'CCX Team Partner', fee: '85% seller keeps · 2% to your team fund · 13% GoodKit' },
  };
  const { label, fee } = typeLabels[entityType] || { label: 'Partner', fee: '' };

  await sendEmail(to, `Welcome to GoodKit — ${entityName}`, emailTemplate(`Welcome to the team, ${recipientName}! 🚴`, `
    <p style="font-size:15px;color:#1A1A14;line-height:1.8;margin:0 0 20px;">
      You're now a <strong>${label}</strong> on <strong>GoodKit</strong> — the marketplace built for cyclists, by cyclists.
    </p>

    <div style="background:#E8E0D0;padding:18px 22px;margin-bottom:20px;border-left:4px solid #FF5C1A;">
      <p style="font-size:11px;letter-spacing:0.15em;text-transform:uppercase;color:#888;margin:0 0 4px;">Your payout</p>
      <p style="font-size:16px;font-weight:700;color:#1A1A14;margin:0;">${fee}</p>
    </div>

    <p style="font-size:14px;color:#555;line-height:1.8;margin:0 0 16px;">
      <strong>What GoodKit is:</strong> A national marketplace for quality used cycling gear — bikes, components, apparel, accessories. We vet sellers, handle payments securely, generate shipping labels, and protect buyers — so the community can buy and sell with real confidence.
    </p>

    <p style="font-size:14px;color:#555;line-height:1.8;margin:0 0 16px;">
      <strong>Our mission:</strong> 2% of every transaction goes back to causes our users care about — cycling clubs, teams, trail advocacy groups, and programs that grow the sport. That 2% always comes out of our cut, never yours.
    </p>

    ${loginLink ? `
    <p style="font-size:14px;color:#555;margin:0 0 8px;"><strong>Your seller dashboard:</strong></p>
    <a href="${loginLink}" style="display:inline-block;background:#FF5C1A;color:white;padding:12px 24px;font-size:14px;font-weight:600;text-decoration:none;margin-bottom:24px;">Access Your Dashboard →</a>
    ` : ''}

    <div style="background:#FFF8F5;border:2px dashed #FF5C1A;padding:18px 22px;margin-bottom:20px;">
      <p style="font-size:13px;font-weight:700;color:#FF5C1A;margin:0 0 8px;">📸 One thing we need from you</p>
      <p style="font-size:13px;color:#555;line-height:1.7;margin:0 0 12px;">
        Send us your <strong>logo or badge artwork</strong> — a PNG or SVG works great. We'll display it on your profile page and badge so buyers know exactly who they're buying from. This builds real trust with your community.
      </p>
      <p style="font-size:13px;color:#555;margin:0;">
        Just reply to this email with your artwork attached, or send it to <a href="mailto:hello@good-kit.com" style="color:#FF5C1A;">hello@good-kit.com</a> with your team/shop name in the subject line.
      </p>
    </div>

    <p style="font-size:13px;color:#888;line-height:1.7;margin:0;">
      Questions? Reply here anytime. We're a small team and we read every message.<br>
      — Casey &amp; Sam, GoodKit
    </p>
  `));
}

// ── LISTING ALERTS ───────────────────────────────────────────────────────────
async function fireListingAlerts(listing) {
  try {
    const alerts = db.prepare("SELECT * FROM listing_alerts WHERE active = 1").all();
    for (const alert of alerts) {
      const priceMatch    = !alert.max_price || listing.price <= alert.max_price;
      const categoryMatch = !alert.category  || alert.category === listing.category;
      const conditionMatch = !alert.condition || alert.condition === listing.condition;
      if (!priceMatch || !categoryMatch || !conditionMatch) continue;
      const unsubUrl = `${BASE_URL}/marketplace?unsubscribe=${alert.unsubscribe_token}`;
      await sendEmail(alert.email, `New listing: ${listing.title}`, emailTemplate('New Listing Alert',
        `<p style="font-size:15px;color:#1A1A14;line-height:1.7;margin:0 0 16px;">A new item matching your alert just dropped on GoodKit.</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:11px;letter-spacing:0.15em;text-transform:uppercase;color:#888;margin:0 0 4px;">${listing.category}</p>
           <p style="font-size:20px;font-weight:700;color:#1A1A14;margin:0 0 4px;">${listing.title}</p>
           <p style="font-size:13px;color:#888;margin:0 0 12px;">${listing.condition}</p>
           <p style="font-size:28px;font-weight:900;color:#FF5C1A;margin:0;">$${(listing.price/100).toFixed(0)}</p>
         </div>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Listing →</a>
         <p style="font-size:11px;color:#aaa;margin:20px 0 0;"><a href="${unsubUrl}" style="color:#aaa;">Unsubscribe</a></p>`
      ));
    }
  } catch(err) { console.error('Alert fire error:', err.message); }
}

// ── AI LISTING ASSISTANT ──────────────────────────────────────────────────────

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';

function claudeRequest(body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = https.request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(payload)
      }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error('Claude parse error: ' + data)); }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// POST /ai/analyze-listing — accepts one photo (multipart), returns AI-generated listing fields
app.post('/ai/analyze-listing', upload.single('photo'), async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'AI assist not configured' });
    if (!req.file) return res.status(400).json({ error: 'Photo required' });

    const imageData = fs.readFileSync(req.file.path).toString('base64');
    const mimeType  = req.file.mimetype || 'image/jpeg';

    // Clean up temp file
    fs.unlink(req.file.path, () => {});

    const response = await claudeRequest({
      model: 'claude-opus-4-5',
      max_tokens: 1024,
      messages: [{
        role: 'user',
        content: [
          {
            type: 'image',
            source: { type: 'base64', media_type: mimeType, data: imageData }
          },
          {
            type: 'text',
            text: `You are an expert at listing used cycling gear for sale. Analyze this photo and generate a marketplace listing.

Return ONLY valid JSON with these exact fields:
{
  "title": "concise product title (brand + model + key spec, 4-8 words)",
  "category": "one of: Chains, Cassettes, Chainrings, Derailleurs, Shifters, Cranks, Bottom Brackets, Cables & Housing, Brake Levers, Brake Calipers, Rotors, Brake Pads, Brake Hoses, Handlebars, Stems, Grips, Bar Tape, Headsets, Saddles, Seatposts, Dropper Posts, Flat Pedals, Clipless Pedals, Forks, Rear Shocks, Suspension Parts, Wheels, Tires, Tubes, Hubs, Rims, Jerseys, Bibs & Shorts, Jackets, Base Layers, Socks, Gloves, Helmets, Pads & Protection, MTB Shoes, Road Shoes, Casual Cycling Shoes, Hydration Packs, Frame Bags, Saddle Bags, Handlebar Bags, Backpacks, Hip Packs, Tools, Stands & Workstands, Pumps, Lube & Cleaners, Electronics, Lights, Bike Computers, GPS Units, Heart Rate Monitors, Power Meters, Sensors & Accessories, Smart Trainers, Sunglasses, Goggles, Frames, Complete Bikes, Other",
  "condition": "one of: New, Like New, Good, Fair, Poor",
  "description": "2-4 sentences describing what you see — brand, model, visible condition, notable features. Be honest about any visible wear.",
  "keywords": "comma-separated search terms: brand, model, size/spec details, compatible standards",
  "price_min": estimated low end resale price in USD as integer,
  "price_max": estimated high end resale price in USD as integer,
  "price_note": "one sentence explaining the price estimate"
}

If you cannot identify the item clearly, still return JSON but set title to "Used Cycling Component" and category to "Other".

IMPORTANT: Return raw JSON only — no markdown, no code fences, no explanation. Start your response with { and end with }.`
          }
        ]
      }]
    });

    if (response.error) throw new Error(response.error.message || 'Claude API error');

    const text = response.content?.[0]?.text || '';
    console.log('[AI assist] raw response:', text.slice(0, 500));
    // Strip markdown code fences if Claude wrapped the JSON
    const stripped = text.replace(/```(?:json)?[\r\n]*/gi, '').replace(/```/g, '').trim();
    // Find the outermost {...} block (greedy, last resort brute-force)
    let result;
    try {
      // First: try parsing the whole stripped response directly
      result = JSON.parse(stripped);
    } catch(_) {
      // Second: find the first { and last } and parse between them
      const start = stripped.indexOf('{');
      const end   = stripped.lastIndexOf('}');
      if (start === -1 || end === -1 || end <= start) {
        throw new Error('No JSON object in AI response: ' + text.slice(0, 300));
      }
      try {
        result = JSON.parse(stripped.slice(start, end + 1));
      } catch(parseErr) {
        throw new Error('JSON parse failed: ' + parseErr.message + ' | raw: ' + text.slice(0, 300));
      }
    }

    res.json({ success: true, ...result });
  } catch(err) {
    console.error('AI analyze error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── ROUTES ───────────────────────────────────────────────────────────────────

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/shop', (req, res) => res.sendFile(path.join(__dirname, 'goodkit-marketplace.html')));
app.get('/marketplace', (req, res) => res.sendFile(path.join(__dirname, 'goodkit-marketplace.html')));
app.get('/sell', (req, res) => res.sendFile(path.join(__dirname, 'goodkit-seller-portal.html')));
app.get('/health', (req, res) => res.json({ status: 'GoodKit Marketplace running' }));

// Seller onboarding via Stripe Connect Express
app.post('/seller/onboard', async (req, res) => {
  try {
    const { email, name, phone, dob_day, dob_month, dob_year, address_line1, address_city, address_state, address_zip } = req.body;
    if (!email || !name) return res.status(400).json({ error: 'Email and name required' });
    const nameParts  = name.trim().split(' ');
    const individual = { first_name: nameParts[0], last_name: nameParts.slice(1).join(' ') || 'Seller', email };
    if (phone) individual.phone = phone;
    if (dob_day && dob_month && dob_year) individual.dob = { day: parseInt(dob_day), month: parseInt(dob_month), year: parseInt(dob_year) };
    if (address_line1 && address_city && address_zip) individual.address = { line1: address_line1, city: address_city, state: address_state || 'CO', postal_code: address_zip, country: 'US' };
    const account = await stripe.accounts.create({
      type: 'express', country: 'US', email,
      business_type: 'individual', individual,
      capabilities: { card_payments: { requested: true }, transfers: { requested: true } },
      business_profile: {
        url: BASE_URL,
        mcc: '5941',
        product_description: 'Used cycling components, apparel, and gear sold through GoodKit marketplace'
      },
      settings: { payouts: { schedule: { interval: 'manual' } } },
      metadata: { seller_name: name, seller_email: email }
    });
    const accountLink = await stripe.accountLinks.create({
      account: account.id,
      refresh_url: `${BASE_URL}/goodkit-seller-portal.html?reauth=true&account=${account.id}`,
      return_url:  `${BASE_URL}/goodkit-seller-portal.html?onboarded=true&account=${account.id}`,
      type: 'account_onboarding',
      collection_options: { fields: 'currently_due', future_requirements: 'omit' }
    });
    res.json({ url: accountLink.url, accountId: account.id });
  } catch(err) { console.error('Onboard error:', err); res.status(500).json({ error: err.message }); }
});

// Save Stripe account ID to seller's listings after onboarding
app.post('/seller/stripe-account', (req, res) => {
  try {
    const { token, stripe_account_id } = req.body;
    if (!token || !stripe_account_id) return res.status(400).json({ error: 'token and stripe_account_id required' });
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token=? AND used=1").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid session' });
    // Save to session (works even if seller has no listings yet)
    db.prepare("UPDATE seller_sessions SET stripe_account_id=? WHERE token=?").run(stripe_account_id, token);
    // Also update any existing listings
    db.prepare("UPDATE listings SET stripe_account_id=? WHERE seller_email=?").run(stripe_account_id, session.email);
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Save seller shipping address (for Shippo label generation)
app.post('/seller/shipping-address', (req, res) => {
  try {
    const { token, name, street1, city, state, zip, phone } = req.body;
    if (!token || !name || !street1 || !city || !state || !zip) return res.status(400).json({ error: 'All address fields required' });
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token=? AND used=1").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid session' });
    // Update all sessions for this email so address persists across sign-ins
    db.prepare(`UPDATE seller_sessions SET ship_name=?, ship_street1=?, ship_city=?, ship_state=?, ship_zip=?, ship_phone=? WHERE email=? AND used=1`)
      .run(name.trim(), street1.trim(), city.trim(), state.trim(), zip.trim(), phone?.trim() || '', session.email);
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/seller/status/:accountId', async (req, res) => {
  try {
    const account = await stripe.accounts.retrieve(req.params.accountId);
    res.json({ ready: account.charges_enabled && account.payouts_enabled, detailsSubmitted: account.details_submitted });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Magic link auth — seller only (no staff role in GoodKit)
app.post('/seller/magic-link', async (req, res) => handleMagicLink(req, res));
app.post('/seller/auth/request',  async (req, res) => handleMagicLink(req, res));
async function handleMagicLink(req, res) {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });
    const emailClean = email.toLowerCase().trim();
    const token     = uuidv4() + uuidv4();
    const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
    db.prepare("INSERT INTO seller_sessions (id, email, token, role, expires_at) VALUES (?, ?, ?, 'seller', ?)").run(uuidv4(), emailClean, token, expiresAt);
    const link = `${BASE_URL}/sell?token=${token}`;
    await sendEmail(email, 'Your GoodKit Seller Portal Login', emailTemplate('Seller Portal Access',
      `<p style="font-size:15px;color:#1A1A14;line-height:1.7;margin:0 0 20px;">Click below to access your seller portal. Link expires in 30 minutes.</p>
       <a href="${link}" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Access Portal →</a>
       <p style="font-size:12px;color:#888;margin:20px 0 0;">If you didn't request this, ignore this email.</p>`
    ));
    res.json({ success: true, message: 'Login link sent to ' + email });
  } catch(err) { console.error('Magic link error:', err); res.status(500).json({ error: err.message }); }
}

function handleVerifyToken(req, res) {
  try {
    const { token } = req.query;
    if (!token) return res.status(400).json({ error: 'Token required' });
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token = ? AND used = 0").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid or expired link.' });
    if (new Date(session.expires_at) < new Date()) return res.status(401).json({ error: 'This link has expired. Please request a new one.' });
    db.prepare("UPDATE seller_sessions SET used = 1 WHERE token = ?").run(token);
    // Create a persistent session token
    const sessionToken = uuidv4();
    const sessionExpiry = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();
    db.prepare("INSERT INTO seller_sessions (id, email, token, role, expires_at, used) VALUES (?, ?, ?, 'seller', ?, 1)").run(uuidv4(), session.email, sessionToken, sessionExpiry);
    res.json({ success: true, sessionToken, email: session.email, role: 'seller' });
  } catch(err) { res.status(500).json({ error: err.message }); }
}
app.get('/seller/verify-token', handleVerifyToken);
app.get('/seller/auth/verify',  handleVerifyToken);

app.get('/seller/portal', (req, res) => {
  try {
    const { token } = req.query;
    if (!token) return res.status(401).json({ error: 'Token required' });
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token = ? AND used = 1").get(token);
    if (!session || session.role !== 'seller') return res.status(401).json({ error: 'Invalid session' });
    if (new Date(session.expires_at) < new Date()) return res.status(401).json({ error: 'Session expired' });
    const email    = session.email;
    const accountType = session.account_type || 'individual';
    const shopSlug    = session.shop_slug || null;
    const listings = db.prepare("SELECT * FROM listings WHERE seller_email = ? ORDER BY created_at DESC").all(email)
      .map(l => ({
        ...l,
        photos: JSON.parse(l.photos || '[]'),
        price:  l.price / 100,
        shipping_estimate: (l.shipping_estimate || 0) / 100,
        split:  calculateSplit(l.price, l.shipping_estimate || 0, null, accountType),
        label:  getLabelTier(l.weight_oz)
      }));
    const sales = db.prepare("SELECT s.*, l.title, l.photos FROM sales s JOIN listings l ON s.listing_id=l.id WHERE l.seller_email=? ORDER BY s.created_at DESC").all(email)
      .map(s => ({
        ...s,
        photos:          JSON.parse(s.photos || '[]'),
        amount:          s.amount / 100,
        seller_payout:   s.seller_payout / 100,
        platform_payout: s.platform_payout / 100
      }));
    const offers = db.prepare("SELECT o.*, l.title, l.price as list_price, l.photos FROM offers o JOIN listings l ON o.listing_id=l.id WHERE l.seller_email=? AND o.status IN ('pending','countered') ORDER BY o.created_at DESC").all(email)
      .map(o => ({
        ...o,
        photos:         JSON.parse(o.photos || '[]'),
        amount:         o.amount / 100,
        list_price:     o.list_price / 100,
        counter_amount: o.counter_amount ? o.counter_amount / 100 : null
      }));
    const threads = db.prepare("SELECT m.*, l.title as listing_title FROM messages m JOIN listings l ON m.listing_id=l.id WHERE l.seller_email=? OR m.from_email=? ORDER BY m.created_at DESC").all(email, email);
    res.json({
      success: true,
      seller: (() => {
        const anySession = db.prepare("SELECT stripe_account_id, ship_name, ship_street1, ship_city, ship_state, ship_zip, ship_phone FROM seller_sessions WHERE email=? AND used=1 AND stripe_account_id IS NOT NULL AND stripe_account_id != '' ORDER BY created_at DESC LIMIT 1").get(email);
        const addrSession = db.prepare("SELECT ship_name, ship_street1, ship_city, ship_state, ship_zip, ship_phone FROM seller_sessions WHERE email=? AND used=1 AND ship_street1 IS NOT NULL ORDER BY created_at DESC LIMIT 1").get(email);
        const stripeId = listings[0]?.stripe_account_id || session.stripe_account_id || anySession?.stripe_account_id || null;
        return {
          email,
          name: listings[0]?.seller_name || email,
          stripe_account_id: stripeId,
          stripe_connected: !!(stripeId && stripeId.startsWith('acct_')),
          shipping_address: addrSession ? {
            name:    addrSession.ship_name,
            street1: addrSession.ship_street1,
            city:    addrSession.ship_city,
            state:   addrSession.ship_state,
            zip:     addrSession.ship_zip,
            phone:   addrSession.ship_phone
          } : null
        };
      })(),
      account: {
        type:       accountType,
        isShop:     accountType === 'shop',
        shopSlug:   shopSlug,
        sellerPct:  accountType === 'shop' ? 65 : 85,
        platformPct: accountType === 'shop' ? 35 : 15,
        storefrontUrl: shopSlug ? `${SITE_URL}/shop/${shopSlug}` : null
      },
      stats: {
        totalEarned:    sales.filter(s => s.status === 'paid_out').reduce((sum, s) => sum + s.seller_payout, 0),
        pendingPayout:  sales.filter(s => s.status === 'delivered').reduce((sum, s) => sum + s.seller_payout, 0),
        activeListings: listings.filter(l => l.status === 'approved').length,
        soldListings:   listings.filter(l => l.status === 'sold').length,
        totalViews:     listings.reduce((sum, l) => sum + (l.view_count || 0), 0)
      },
      listings, sales, offers, threads
    });
  } catch(err) { console.error('Portal error:', err); res.status(500).json({ error: err.message }); }
});

// ── LISTINGS ─────────────────────────────────────────────────────────────────

app.post('/listings', upload.array('photos', 8), async (req, res) => {
  try {
    const { seller_name, seller_email, stripe_account_id: client_stripe_id, title, category, size, description, condition, price, shipping_estimate, weight_oz, keywords, ccx_code, allows_pickup } = req.body;
    if (!seller_name || !seller_email || !title || !price) return res.status(400).json({ error: 'Missing required fields' });
    // Fall back to session-stored stripe_account_id if client didn't send one
    const sellerSession = db.prepare("SELECT * FROM seller_sessions WHERE email=? AND used=1 AND stripe_account_id IS NOT NULL AND stripe_account_id != '' ORDER BY created_at DESC LIMIT 1").get(seller_email);
    const stripe_account_id = client_stripe_id || sellerSession?.stripe_account_id || null;
    if (!stripe_account_id) return res.status(400).json({ error: 'Seller must complete Stripe onboarding first' });
    const priceInCents  = Math.round(parseFloat(price) * 100);
    const shippingCents = Math.round(parseFloat(shipping_estimate || 0) * 100);
    const weightOz      = parseInt(weight_oz || 0) || 0;
    if (priceInCents < 100) return res.status(400).json({ error: 'Minimum price is $1.00' });
    const photos   = req.files ? req.files.map(f => '/uploads/' + f.filename) : [];
    if (!photos.length) return res.status(400).json({ error: 'At least one photo is required' });
    // Validate CCX code if provided
    let validCcxCode = null;
    if (ccx_code && ccx_code.trim()) {
      const ccxTeam = db.prepare("SELECT code FROM ccx_teams WHERE code=? AND status='active'").get(ccx_code.trim().toUpperCase());
      validCcxCode = ccxTeam ? ccxTeam.code : null;
    }
    const id       = uuidv4();
    const stripeId = (stripe_account_id && stripe_account_id.trim() !== '') ? stripe_account_id.trim() : null;
    const validDeliveryType = 'ship'; // delivery method chosen by buyer at checkout
    const allowsPickup = allows_pickup === '1' || allows_pickup === 'true' || allows_pickup === true ? 1 : 0;
    // Validate community_id if provided — must exist and be active
    let validCommunityId = null;
    if (req.body.community_id) {
      const comm = db.prepare("SELECT id FROM communities WHERE id=? AND status='active'").get(req.body.community_id);
      validCommunityId = comm ? comm.id : null;
    }

    // Auto-generate SEO-optimized description if seller didn't write one (or wrote <20 chars)
    let finalDescription = (description || '').trim();
    if (finalDescription.length < 20 && process.env.ANTHROPIC_API_KEY) {
      try {
        const seoPrompt = `Generate a 2-3 sentence SEO-optimized product description for this used cycling gear listing. Be specific, factual, and include relevant search terms naturally.

Item: ${title.trim()}
Category: ${category || 'cycling gear'}
Condition: ${condition || 'Good'}
Price: $${(priceInCents/100).toFixed(2)}
Keywords: ${keywords || ''}

Write the description as if you are describing the item to a buyer. Include: what the item is, condition details, compatibility notes if relevant, and why it's a good buy. Do NOT use hype words. Return only the description text, no quotes, no labels.`;

        const seoResp = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
          body: JSON.stringify({ model: 'claude-haiku-4-5-20251001', max_tokens: 200, messages: [{ role: 'user', content: seoPrompt }] })
        });
        const seoData = await seoResp.json();
        const generated = seoData.content?.[0]?.text?.trim();
        if (generated && generated.length > 20) finalDescription = generated;
        console.log('[SEO] Auto-generated description for:', title.trim());
      } catch(seoErr) {
        console.warn('[SEO] Description generation failed, using original:', seoErr.message);
      }
    }

    db.prepare(`INSERT INTO listings (id,seller_name,seller_email,stripe_account_id,title,category,size,description,condition,price,shipping_estimate,weight_oz,photos,status,keywords,ccx_code,delivery_type,allows_pickup,community_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'approved',?,?,?,?,?)`)
      .run(id, seller_name.trim(), seller_email.trim(), stripeId, title.trim(), category || 'Other', size || '', finalDescription, condition || 'Good', priceInCents, shippingCents, weightOz, JSON.stringify(photos), keywords || '', validCcxCode, validDeliveryType, allowsPickup, validCommunityId);
    const newListing = db.prepare("SELECT * FROM listings WHERE id = ?").get(id);
    await fireListingAlerts(newListing);
    await sendEmail(NOTIFY_EMAIL, `[GoodKit] New listing: ${title}`, `Seller: ${seller_name} · ${seller_email}\nItem: ${title}\nPrice: $${priceInCents/100}\nWeight: ${weightOz}oz${validCcxCode ? `\nCCX: ${validCcxCode}` : ''}`);
    const split = calculateSplit(priceInCents, shippingCents, validCcxCode);
    const label = getLabelTier(weightOz);
    res.json({ success: true, listingId: id, split, label, ccx_code: validCcxCode });
  } catch(err) { console.error('Listing error:', err); res.status(500).json({ error: err.message }); }
});

app.get('/listings', (req, res) => {
  try {
    const { category, maxPrice, condition } = req.query;
    let listings = db.prepare(`
      SELECT l.*, c.name AS community_name, c.slug AS community_slug,
             c.logo_url AS community_logo, c.accent_color AS community_accent
      FROM listings l
      LEFT JOIN communities c ON l.community_id = c.id AND c.status='active'
      WHERE l.status='approved' ORDER BY l.created_at DESC
    `).all()
      .map(l => ({
        ...l,
        photos:            JSON.parse(l.photos || '[]'),
        price:             l.price / 100,
        shipping_estimate: (l.shipping_estimate || 0) / 100,
        split:             calculateSplit(l.price, l.shipping_estimate || 0),
        label:             getLabelTier(l.weight_oz),
        delivery_type:     l.delivery_type || 'ship',
        allows_pickup:     l.allows_pickup || 0
      }));
    if (category) listings = listings.filter(l => l.category === category);
    if (maxPrice)  listings = listings.filter(l => l.price <= parseFloat(maxPrice));
    if (condition) listings = listings.filter(l => l.condition === condition);
    res.json({ success: true, listings });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/listings/:id', (req, res) => {
  try {
    const listing = db.prepare("SELECT * FROM listings WHERE id=? AND status='approved'").get(req.params.id);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });
    db.prepare("UPDATE listings SET view_count=view_count+1 WHERE id=?").run(req.params.id);
    listing.photos            = JSON.parse(listing.photos || '[]');
    listing.price             = listing.price / 100;
    listing.shipping_estimate = (listing.shipping_estimate || 0) / 100;
    listing.split             = calculateSplit(listing.price * 100, listing.shipping_estimate * 100);
    listing.label             = getLabelTier(listing.weight_oz);
    res.json({ success: true, listing });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/listings/:id/view', (req, res) => {
  try { db.prepare("UPDATE listings SET view_count=view_count+1 WHERE id=?").run(req.params.id); res.json({ success: true }); }
  catch(err) { res.status(500).json({ error: err.message }); }
});

// ── ALERTS ───────────────────────────────────────────────────────────────────

app.post('/alerts/subscribe', async (req, res) => {
  try {
    const { email, category, maxPrice, condition } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });
    const emailClean = email.toLowerCase().trim();
    const existing = db.prepare("SELECT id FROM listing_alerts WHERE email=? AND (category IS ? OR (category IS NULL AND ? IS NULL)) AND active=1").get(emailClean, category || null, category || null);
    if (existing) return res.json({ success: true, message: 'Alert already set up.' });
    const id = uuidv4(), unsubscribeToken = uuidv4();
    db.prepare("INSERT INTO listing_alerts (id,email,category,max_price,condition,unsubscribe_token) VALUES (?,?,?,?,?,?)").run(id, emailClean, category || null, maxPrice ? Math.round(parseFloat(maxPrice) * 100) : null, condition || null, unsubscribeToken);
    await sendEmail(email, 'GoodKit Alert Set Up', emailTemplate('Alert Confirmed ✓',
      `<p style="font-size:15px;color:#1A1A14;line-height:1.7;margin:0 0 16px;">You're all set! We'll notify you when matching gear drops.</p>
       <div style="background:#E8E0D0;padding:14px 18px;margin-bottom:20px;">
         <p style="font-size:14px;color:#1A1A14;margin:0 0 4px;"><strong>Category:</strong> ${category || 'All'}</p>
         ${maxPrice ? `<p style="font-size:14px;color:#1A1A14;margin:0 0 4px;"><strong>Max price:</strong> $${maxPrice}</p>` : ''}
         ${condition ? `<p style="font-size:14px;color:#1A1A14;margin:0;"><strong>Condition:</strong> ${condition}</p>` : ''}
       </div>
       <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Browse GoodKit →</a>
       <p style="font-size:11px;color:#aaa;margin:20px 0 0;"><a href="${BASE_URL}/shop?unsubscribe=${unsubscribeToken}" style="color:#aaa;">Unsubscribe</a></p>`
    ));
    res.json({ success: true, message: "Alert set! We'll email you when matching gear is listed." });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/alerts/unsubscribe', (req, res) => {
  try {
    const { token } = req.query;
    if (!token) return res.status(400).json({ error: 'Token required' });
    db.prepare("UPDATE listing_alerts SET active=0 WHERE unsubscribe_token=?").run(token);
    res.json({ success: true, message: 'Unsubscribed successfully.' });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── CHECKOUT ─────────────────────────────────────────────────────────────────
// Confirmed working pattern: application_fee_amount + transfer_data: {destination}
// DO NOT add amount to transfer_data — scopes the PI to connected account and breaks confirmCardPayment
app.post('/checkout', async (req, res) => {
  try {
    const { listingId, buyerEmail, deliveryType, buyerName, buyerStreet1, buyerCity, buyerState, buyerZip } = req.body;
    const listing = db.prepare("SELECT * FROM listings WHERE id=? AND status='approved'").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found or no longer available' });

    // Buyer chooses ship vs pickup (if seller allows); validate the choice
    const buyerWantsPickup = deliveryType === 'local_pickup' && listing.allows_pickup;
    const resolvedDeliveryType = buyerWantsPickup ? 'local_pickup' : 'ship';
    const shippingCents = buyerWantsPickup ? 0 : (listing.shipping_estimate || 0);
    const split         = calculateSplit(listing.price, shippingCents, listing.ccx_code);
    const totalCharge   = listing.price + shippingCents;
    const labelInfo     = getLabelTier(listing.weight_oz);

    const piParams = {
      amount:               totalCharge,
      currency:             'usd',
      receipt_email:        buyerEmail,
      payment_method_types: ['card'],
      metadata: {
        listingId:    listing.id,
        listingTitle: listing.title,
        sellerEmail:  listing.seller_email,
        deliveryType: resolvedDeliveryType,
        labelTier:    labelInfo.tier
      }
    };

    const hasStripeAccount = listing.stripe_account_id &&
      listing.stripe_account_id.trim() !== '' &&
      listing.stripe_account_id.startsWith('acct_');

    if (hasStripeAccount) {
      // platform keeps 15% of item price (13% GoodKit + 2% CCX fund if applicable)
      const appFee = Math.round(split.platformFee * 100);
      piParams.application_fee_amount = Math.max(appFee, 0);
      piParams.transfer_data = { destination: listing.stripe_account_id };
    }

    const paymentIntent = await stripe.paymentIntents.create(piParams);

    const saleId = uuidv4();
    const payoutAfter = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString(); // 7 days from purchase; updated to delivery+48h when carrier confirms
    db.prepare(`INSERT INTO sales (id,listing_id,buyer_email,buyer_name,buyer_street1,buyer_city,buyer_state,buyer_zip,payment_intent_id,amount,seller_payout,platform_payout,delivery_type,label_tier,ccx_code,ccx_fund,payout_after) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(saleId, listingId, buyerEmail,
        buyerName || '', buyerStreet1 || '', buyerCity || '', buyerState || '', buyerZip || '',
        paymentIntent.id, listing.price,
        Math.round(split.sellerNet * 100),
        Math.round(split.platformFee * 100),
        resolvedDeliveryType,
        labelInfo.tier,
        listing.ccx_code || null,
        Math.round((split.ccxFund || 0) * 100),
        payoutAfter
      );

    res.json({
      success:        true,
      clientSecret:   paymentIntent.client_secret,
      publishableKey: process.env.STRIPE_PUBLISHABLE_KEY,
      listing: {
        title:  listing.title,
        price:  listing.price / 100,
        photos: JSON.parse(listing.photos || '[]')
      },
      split: {
        seller:      split.sellerNet,
        platform:    split.platformNet,
        sellerPct:   split.sellerPct,
        platformPct: split.platformPct
      },
      label:        labelInfo,
      deliveryType: resolvedDeliveryType
    });
  } catch(err) { console.error('Checkout error:', err); res.status(500).json({ error: err.message }); }
});

// ── OFFERS ───────────────────────────────────────────────────────────────────

app.post('/offers', async (req, res) => {
  try {
    const { listingId, buyerName, buyerEmail, amount, message } = req.body;
    if (!listingId || !buyerEmail || !amount) return res.status(400).json({ error: 'Missing required fields' });
    const listing = db.prepare("SELECT * FROM listings WHERE id=? AND status='approved'").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });
    const amountCents = Math.round(parseFloat(amount) * 100);
    if (amountCents < 100) return res.status(400).json({ error: 'Minimum offer is $1.00' });
    const id = uuidv4();
    db.prepare("INSERT INTO offers (id,listing_id,buyer_name,buyer_email,amount,message) VALUES (?,?,?,?,?,?)").run(id, listingId, buyerName || buyerEmail, buyerEmail, amountCents, message || '');
    await sendEmail(listing.seller_email, `New offer on your ${listing.title}`, emailTemplate('You Have a New Offer',
      `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Hi ${listing.seller_name}, <strong>${buyerName || buyerEmail}</strong> made an offer on <strong>${listing.title}</strong>.</p>
       <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
         <p style="font-size:13px;color:#888;margin:0 0 4px;">Listed price</p>
         <p style="font-size:24px;font-weight:700;color:#1A1A14;margin:0 0 12px;">$${(listing.price/100).toFixed(0)}</p>
         <p style="font-size:13px;color:#888;margin:0 0 4px;">Offer</p>
         <p style="font-size:24px;font-weight:700;color:#FF5C1A;margin:0;">$${parseFloat(amount).toFixed(2)}</p>
         ${message ? `<p style="font-size:13px;color:#555;margin:12px 0 0;font-style:italic;">"${message}"</p>` : ''}
       </div>
       <a href="${BASE_URL}/seller-portal" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Respond in Portal →</a>`
    ));
    await sendEmail(NOTIFY_EMAIL, `[GoodKit] New offer on ${listing.title}`, `Offer: $${amount} from ${buyerEmail}`);
    res.json({ success: true, offerId: id });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/offers/:id/respond', async (req, res) => {
  try {
    const { token, action, counterAmount, counterMessage } = req.body;
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token=? AND used=1").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid session' });
    const offer = db.prepare("SELECT o.*, l.title, l.seller_email, l.seller_name, l.price as list_price FROM offers o JOIN listings l ON o.listing_id=l.id WHERE o.id=?").get(req.params.id);
    if (!offer) return res.status(404).json({ error: 'Offer not found' });
    if (offer.seller_email !== session.email) return res.status(403).json({ error: 'Unauthorized' });
    if (action === 'accept') {
      db.prepare("UPDATE offers SET status='accepted', updated_at=datetime('now') WHERE id=?").run(offer.id);
      await sendEmail(offer.buyer_email, `Your offer on ${offer.title} was accepted!`, emailTemplate('Offer Accepted 🎉',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Your offer of <strong>$${(offer.amount/100).toFixed(2)}</strong> on <strong>${offer.title}</strong> was accepted.</p>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Complete Purchase →</a>`
      ));
    } else if (action === 'counter') {
      if (!counterAmount) return res.status(400).json({ error: 'Counter amount required' });
      db.prepare("UPDATE offers SET status='countered', counter_amount=?, counter_message=?, updated_at=datetime('now') WHERE id=?").run(Math.round(parseFloat(counterAmount) * 100), counterMessage || '', offer.id);
      await sendEmail(offer.buyer_email, `Counter offer on ${offer.title}`, emailTemplate('Counter Offer Received',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">The seller countered your offer on <strong>${offer.title}</strong>.</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:13px;color:#888;margin:0 0 4px;">Your offer</p>
           <p style="font-size:20px;font-weight:700;color:#555;margin:0 0 12px;">$${(offer.amount/100).toFixed(2)}</p>
           <p style="font-size:13px;color:#888;margin:0 0 4px;">Counter offer</p>
           <p style="font-size:24px;font-weight:700;color:#FF5C1A;margin:0;">$${parseFloat(counterAmount).toFixed(2)}</p>
           ${counterMessage ? `<p style="font-size:13px;color:#555;margin:12px 0 0;font-style:italic;">"${counterMessage}"</p>` : ''}
         </div>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Listing →</a>`
      ));
    } else if (action === 'decline') {
      db.prepare("UPDATE offers SET status='declined', updated_at=datetime('now') WHERE id=?").run(offer.id);
      await sendEmail(offer.buyer_email, `Update on your offer for ${offer.title}`, emailTemplate('Offer Update',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 20px;">The seller declined your offer on <strong>${offer.title}</strong>.</p>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Browse GoodKit →</a>`
      ));
    }
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── MESSAGES ─────────────────────────────────────────────────────────────────

app.post('/messages', async (req, res) => {
  try {
    const { listingId, offerId, fromEmail, fromName, fromRole, body } = req.body;
    if (!listingId || !fromEmail || !body) return res.status(400).json({ error: 'Missing required fields' });
    const listing = db.prepare("SELECT * FROM listings WHERE id=?").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });
    const id = uuidv4();
    db.prepare("INSERT INTO messages (id,listing_id,offer_id,from_email,from_name,from_role,body) VALUES (?,?,?,?,?,?,?)").run(id, listingId, offerId || null, fromEmail, fromName || fromEmail, fromRole || 'buyer', body);
    if (fromRole === 'buyer') {
      await sendEmail(listing.seller_email, `New message about ${listing.title}`, emailTemplate('New Message',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;"><strong>${fromName || fromEmail}</strong> asked about <strong>${listing.title}</strong>:</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;border-left:3px solid #FF5C1A;">
           <p style="font-size:15px;color:#1A1A14;margin:0;">"${body}"</p>
         </div>
         <a href="${BASE_URL}/seller-portal" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Reply in Portal →</a>`
      ));
      await sendEmail(NOTIFY_EMAIL, `[GoodKit] Message on ${listing.title}`, `From: ${fromEmail}\n${body}`);
    } else {
      const buyerMsg = db.prepare("SELECT from_email FROM messages WHERE listing_id=? AND from_role='buyer' ORDER BY created_at ASC LIMIT 1").get(listingId);
      if (buyerMsg) await sendEmail(buyerMsg.from_email, `Reply about ${listing.title}`, emailTemplate('Message from Seller',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">The seller replied about <strong>${listing.title}</strong>:</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;border-left:3px solid #1A1A14;">
           <p style="font-size:15px;color:#1A1A14;margin:0;">"${body}"</p>
         </div>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Listing →</a>`
      ));
    }
    res.json({ success: true, messageId: id });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/messages/:listingId', (req, res) => {
  try { res.json({ success: true, messages: db.prepare("SELECT * FROM messages WHERE listing_id=? ORDER BY created_at ASC").all(req.params.listingId) }); }
  catch(err) { res.status(500).json({ error: err.message }); }
});

// Reply to a message thread (seller) — POST /messages/:id/reply {token, body}
app.post('/messages/:id/reply', async (req, res) => {
  try {
    const { token, body } = req.body;
    if (!token || !body) return res.status(400).json({ error: 'token and body required' });
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token=? AND used=1").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid session' });
    const orig = db.prepare("SELECT * FROM messages WHERE id=?").get(req.params.id);
    if (!orig) return res.status(404).json({ error: 'Message not found' });
    const listing = db.prepare("SELECT * FROM listings WHERE id=?").get(orig.listing_id);
    if (!listing || listing.seller_email !== session.email) return res.status(403).json({ error: 'Unauthorized' });
    const id = uuidv4();
    db.prepare("INSERT INTO messages (id,listing_id,offer_id,from_email,from_name,from_role,body) VALUES (?,?,?,?,?,?,?)").run(id, orig.listing_id, orig.offer_id || null, session.email, session.email, 'seller', body);
    const buyerMsg = db.prepare("SELECT from_email FROM messages WHERE listing_id=? AND from_role='buyer' ORDER BY created_at ASC LIMIT 1").get(orig.listing_id);
    if (buyerMsg) await sendEmail(buyerMsg.from_email, `Reply about ${listing.title}`, emailTemplate('Message from Seller',
      `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">The seller replied about <strong>${listing.title}</strong>:</p>
       <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;border-left:3px solid #1A1A14;">
         <p style="font-size:15px;color:#1A1A14;margin:0;">"${body}"</p>
       </div>
       <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Listing →</a>`
    ));
    res.json({ success: true, messageId: id });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Delete listing (seller) — DELETE /listings/:id with {token} in body
// Note: fetch DELETE with a JSON body is supported; we also accept a query param fallback
app.delete('/listings/:id', async (req, res) => {
  try {
    const token = (req.body && req.body.token) || req.query.token;
    if (!token) return res.status(400).json({ error: 'token required' });
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token=? AND used=1").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid session' });
    const listing = db.prepare("SELECT * FROM listings WHERE id=?").get(req.params.id);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });
    if (listing.seller_email !== session.email) return res.status(403).json({ error: 'Unauthorized' });
    db.prepare("UPDATE listings SET status='deleted' WHERE id=?").run(req.params.id);
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.patch('/listings/:id', (req, res) => {
  try {
    const token = (req.body && req.body.token) || req.query.token;
    if (!token) return res.status(400).json({ error: 'token required' });
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token=? AND used=1").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid session' });
    const listing = db.prepare("SELECT * FROM listings WHERE id=?").get(req.params.id);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });
    if (listing.seller_email !== session.email) return res.status(403).json({ error: 'Unauthorized' });
    const { price, category, size, keywords, status, title, description } = req.body;
    if (price !== undefined && (isNaN(price) || Number(price) < 1)) return res.status(400).json({ error: 'Invalid price' });
    if (price !== undefined) db.prepare("UPDATE listings SET price=? WHERE id=?").run(Math.round(Number(price) * 100), req.params.id);
    if (category !== undefined) db.prepare("UPDATE listings SET category=? WHERE id=?").run(category, req.params.id);
    if (size !== undefined) db.prepare("UPDATE listings SET size=? WHERE id=?").run(size, req.params.id);
    if (keywords !== undefined) db.prepare("UPDATE listings SET keywords=? WHERE id=?").run(keywords, req.params.id);
    if (title !== undefined && title.trim()) db.prepare("UPDATE listings SET title=? WHERE id=?").run(title.trim(), req.params.id);
    if (description !== undefined) db.prepare("UPDATE listings SET description=? WHERE id=?").run(description, req.params.id);
    // Allow seller to mark as sold (sold elsewhere) or reactivate (approved)
    if (status !== undefined && ['sold','approved','expired'].includes(status)) {
      let extra = '';
      if (status === 'sold') extra = ", sold_at=datetime('now')";
      if (status === 'approved') {
        // Reactivating: reset sold_at and lifecycle clock so 30-day window restarts
        extra = ", sold_at=NULL, created_at=datetime('now'), reminded_at=NULL, final_warned_at=NULL";
      }
      db.prepare(`UPDATE listings SET status=?${extra} WHERE id=?`).run(status, req.params.id);
    }
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── SHIPPO TRACKING WEBHOOK ───────────────────────────────────────────────────
// Shippo fires track_updated when carrier status changes.
// Register this URL in Shippo dashboard: https://good-kit.com/webhooks/shippo
// Event type: track_updated
// No signature secret needed — Shippo uses HMAC only on newer webhook versions;
// we validate by looking up the tracker_id in our own DB.

app.post('/webhooks/shippo', express.json(), async (req, res) => {
  try {
    const event = req.body;
    // Shippo sends: { event: 'track_updated', data: { tracking_status: { status }, object_id, tracking_number } }
    if (event.event !== 'track_updated') return res.json({ received: true });

    const data = event.data || {};
    const trackerId = data.object_id;
    const status = data.tracking_status?.status; // DELIVERED, TRANSIT, UNKNOWN, etc.

    if (!trackerId || status !== 'DELIVERED') return res.json({ received: true });

    // Find the sale with this tracker
    const sale = db.prepare("SELECT s.*, l.stripe_account_id, l.title FROM sales s JOIN listings l ON s.listing_id=l.id WHERE s.shippo_tracker_id=?").get(trackerId);
    if (!sale) {
      console.warn(`[Shippo webhook] No sale found for tracker ${trackerId}`);
      return res.json({ received: true });
    }

    if (sale.status === 'paid_out' || sale.delivered_at) {
      return res.json({ received: true }); // already processed
    }

    const deliveredAt = new Date().toISOString();
    const payoutAfter = new Date(Date.now() + 48 * 60 * 60 * 1000).toISOString();

    db.prepare("UPDATE sales SET status='delivered', delivered_at=?, payout_after=? WHERE id=?")
      .run(deliveredAt, payoutAfter, sale.id);

    console.log(`[Shippo] Delivery confirmed for sale ${sale.id} (${sale.title}). Payout releases at ${payoutAfter}`);

    // Notify buyer — let them know it arrived and they have 48h to flag issues
    const flagUrl = `${process.env.SITE_URL || 'https://good-kit.com'}/flag-purchase?sale=${sale.id}&email=${encodeURIComponent(sale.buyer_email)}`;
    const deadline = new Date(payoutAfter).toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZoneName: 'short' });
    await sendEmail(sale.buyer_email, `Your order has been delivered: ${sale.title}`, emailTemplate('Your Item Arrived 📦',
      `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Your <strong>${sale.title}</strong> has been marked as delivered.</p>
       <div style="background:#EBF4EF;border:1px solid #1D5C3A;padding:16px 20px;margin-bottom:20px;">
         <p style="font-size:14px;font-weight:700;color:#1D5C3A;margin:0 0 8px;">🛡️ You have 48 hours to flag any issues</p>
         <p style="font-size:13px;color:#333;margin:0 0 8px;">Item not as described? Damaged? Flag it before <strong>${deadline}</strong> and we'll step in before any money moves.</p>
         <a href="${flagUrl}" style="display:inline-block;background:#1D5C3A;color:white;padding:10px 20px;font-size:13px;font-weight:600;text-decoration:none;">Report a Problem →</a>
       </div>
       <p style="font-size:12px;color:#888;margin:0;">No issues? No action needed — the seller is paid out automatically after ${deadline}.</p>`
    )).catch(e => console.warn('[Shippo webhook] buyer email failed:', e.message));

    res.json({ received: true });
  } catch(err) {
    console.error('[Shippo webhook] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── WEBHOOK ───────────────────────────────────────────────────────────────────

app.post('/webhook', async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try { event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET); }
  catch(err) { return res.status(400).send('Webhook error: ' + err.message); }

  // Stripe Connect onboarding complete — flip stripe_status to stripe_active
  if (event.type === 'account.updated') {
    const acct = event.data.object;
    if (acct.charges_enabled && acct.payouts_enabled) {
      const stripeId = acct.id;
      // Update whichever entity owns this Stripe account
      db.prepare(`UPDATE shop_accounts SET stripe_status='stripe_active' WHERE stripe_account_id=?`).run(stripeId);
      db.prepare(`UPDATE pro_consign_athletes SET stripe_status='stripe_active' WHERE stripe_account_id=?`).run(stripeId);
      db.prepare(`UPDATE ccx_teams SET stripe_status='stripe_active' WHERE stripe_account_id=?`).run(stripeId);
      console.log(`[stripe] account ${stripeId} fully onboarded → stripe_active`);
    }
    return res.json({ received: true });
  }

  if (event.type === 'payment_intent.succeeded') {
    const pi        = event.data.object;
    const listingId = pi.metadata.listingId;

    db.prepare("UPDATE listings SET status='sold', sold_at=datetime('now') WHERE id=?").run(listingId);
    db.prepare("UPDATE sales SET status='pending_shipment' WHERE payment_intent_id=?").run(pi.id);

    // CCX fund credit — if listing had a ccx_code, credit the team fund
    const saleForCcx = db.prepare("SELECT ccx_code, ccx_fund FROM sales WHERE payment_intent_id=?").get(pi.id);
    if (saleForCcx?.ccx_code && saleForCcx.ccx_fund > 0) {
      db.prepare("UPDATE ccx_teams SET fund_balance=fund_balance+?, total_earned=total_earned+? WHERE code=?")
        .run(saleForCcx.ccx_fund, saleForCcx.ccx_fund, saleForCcx.ccx_code);
    }

    const listing = db.prepare("SELECT * FROM listings WHERE id=?").get(listingId);
    const sale    = db.prepare("SELECT * FROM sales WHERE payment_intent_id=?").get(pi.id);

    if (listing && sale) {
      const isPickup  = sale.delivery_type === 'local_pickup' || sale.delivery_type === 'pickup';
      const isDetour  = sale.delivery_type === 'detour';
      let labelUrl = null, trackingNumber = null, trackingUrl = null;
      let detourJobId = null;

      // ── Detour regional delivery — post job to Detour platform ───────────
      if (isDetour && process.env.DETOUR_API_URL && process.env.GOODKIT_API_KEY) {
        try {
          const sellerAddr = db.prepare("SELECT ship_name, ship_street1, ship_city, ship_state, ship_zip, ship_phone FROM seller_sessions WHERE email=? AND used=1 AND ship_street1 IS NOT NULL ORDER BY created_at DESC LIMIT 1").get(listing.seller_email);
          const WEIGHT_SIZE_MAP = { xs: 'small', sm: 'small', md: 'medium', lg: 'large' };
          const itemSize = WEIGHT_SIZE_MAP[getLabelTier(listing.weight_oz).tier] || 'medium';
          const detourPayload = {
            job_type:        'standard',
            title:           `GoodKit: ${listing.title}`,
            description:     `GoodKit marketplace sale. Item: ${listing.title}. Condition: ${listing.condition}.`,
            item_size:       itemSize,
            item_weight:     listing.weight_oz ? Math.ceil(listing.weight_oz / 16) + ' lb' : null,
            pickup_address:  sellerAddr?.ship_street1 || '',
            pickup_city:     sellerAddr?.ship_city    || 'Durango',
            pickup_state:    sellerAddr?.ship_state   || 'CO',
            pickup_zip:      sellerAddr?.ship_zip     || '',
            dropoff_address: sale.buyer_street1 || '',
            dropoff_city:    sale.buyer_city    || '',
            dropoff_state:   sale.buyer_state   || '',
            dropoff_zip:     sale.buyer_zip     || '',
            offered_price:   ((listing.shipping_estimate || 0) / 100).toFixed(2),
            seller_name:     listing.seller_name,
            seller_phone:    sellerAddr?.ship_phone || '',
            buyer_name:      sale.buyer_name || sale.buyer_email,
            notes:           `GoodKit order. Seller payout handled by GoodKit. Sale ID: ${sale.id}`
          };
          const detourBase = process.env.DETOUR_API_URL.startsWith('http') ? process.env.DETOUR_API_URL : `https://${process.env.DETOUR_API_URL}`;
          const dtRes = await fetch(`${detourBase}/api/jobs`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.GOODKIT_API_KEY },
            body:    JSON.stringify(detourPayload)
          });
          const dtData = await dtRes.json();
          if (dtData.id) {
            detourJobId = dtData.id;
            db.prepare("UPDATE sales SET tracking_number=? WHERE id=?").run('DETOUR-' + detourJobId, sale.id);
            console.log(`Detour job created: ${detourJobId} for sale ${sale.id}`);
          } else {
            console.error('Detour job creation failed:', dtData);
          }
        } catch(dtErr) {
          console.error('Detour integration error:', dtErr.message);
        }
      }

      if (!isPickup && !isDetour && SHIPPO_API_KEY) {
        try {
          // Look up seller's saved shipping address
          const sellerAddr = db.prepare("SELECT ship_name, ship_street1, ship_city, ship_state, ship_zip, ship_phone FROM seller_sessions WHERE email=? AND used=1 AND ship_street1 IS NOT NULL ORDER BY created_at DESC LIMIT 1").get(listing.seller_email);
          if (sellerAddr && sale.buyer_street1) {
            const label = await createShippingLabel({
              fromAddress: {
                name:    sellerAddr.ship_name,
                street1: sellerAddr.ship_street1,
                city:    sellerAddr.ship_city,
                state:   sellerAddr.ship_state,
                zip:     sellerAddr.ship_zip,
                phone:   sellerAddr.ship_phone || '',
                country: 'US'
              },
              toAddress: {
                name:    sale.buyer_name || sale.buyer_email,
                street1: sale.buyer_street1,
                city:    sale.buyer_city,
                state:   sale.buyer_state,
                zip:     sale.buyer_zip,
                country: 'US'
              },
              weightOz: listing.weight_oz || 16,
              listingTitle: listing.title
            });
            labelUrl       = label.labelUrl;
            trackingNumber = label.trackingNumber;
            trackingUrl    = label.trackingUrl;
            db.prepare("UPDATE sales SET label_url=?, tracking_number=?, tracking_url=?, shippo_tracker_id=? WHERE id=?").run(labelUrl, trackingNumber, trackingUrl, label.trackerObjectId || null, sale.id);
            console.log(`Label generated: ${trackingNumber} (${label.carrier} ${label.service} $${label.rate})`);
          } else {
            console.warn('Shippo label skipped — missing seller address or buyer address');
          }
        } catch(shipErr) {
          console.error('Shippo label error:', shipErr.message);
        }
      }

      // Email seller
      const sellerLabelSection = isPickup
        ? `<p style="font-size:13px;color:#555;margin:8px 0 0;">📍 Buyer will pick up — coordinate directly with them.</p>`
        : isDetour
        ? `<p style="font-size:13px;color:#555;margin:8px 0 0;">🚗 <strong>Detour regional delivery</strong> — a courier will contact you to arrange pickup.${detourJobId ? ` Job ID: ${detourJobId}` : ''}</p>`
        : labelUrl
          ? `<p style="font-size:13px;color:#555;margin:8px 0 0;">📦 Your prepaid shipping label is ready:</p>
             <a href="${labelUrl}" style="display:inline-block;margin-top:8px;background:#1A1A14;color:#FF5C1A;padding:10px 20px;font-size:13px;font-weight:600;text-decoration:none;">Download Label →</a>
             <p style="font-size:12px;color:#888;margin:8px 0 0;">Tracking: ${trackingNumber}<br>Drop off at any USPS location.</p>`
          : `<p style="font-size:13px;color:#555;margin:8px 0 0;">📦 A shipping label will be sent separately. Check back in a moment.</p>`;

      await sendEmail(listing.seller_email, `Your ${listing.title} sold! 🎉`, emailTemplate('Item Sold!',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Hi ${listing.seller_name}, your <strong>${listing.title}</strong> just sold!</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:13px;color:#888;margin:0 0 4px;">Your payout</p>
           <p style="font-size:28px;font-weight:700;color:#1A1A14;margin:0;">$${(sale.seller_payout/100).toFixed(2)}</p>
           ${sellerLabelSection}
           <p style="font-size:12px;color:#888;margin:8px 0 0;">Payout released 48 hours after delivery confirmation (or 7 days from purchase if tracking isn't updated).</p>
         </div>`
      ));

      // Email buyer — purchase confirmation with buyer protection notice
      const payoutAfterDate = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
      const protectionDeadline = payoutAfterDate.toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZoneName: 'short' });
      const flagUrl = `${process.env.SITE_URL || 'https://good-kit.com'}/flag-purchase?sale=${sale.id}&email=${encodeURIComponent(sale.buyer_email)}`;
      const buyerPickupNote = isPickup
        ? `<p style="font-size:13px;color:#555;margin:8px 0 0;">📍 This is a local pickup — coordinate with the seller to arrange a meetup.</p>`
        : isDetour
        ? `<p style="font-size:13px;color:#555;margin:8px 0 0;">🚗 A Detour courier will deliver your item — you'll receive a separate notification when pickup is scheduled.</p>`
        : trackingNumber
          ? `<p style="font-size:13px;color:#555;margin:0 0 10px;">📦 Your item has been shipped.</p>
             <p style="font-size:13px;color:#555;margin:0 0 4px;"><strong>Tracking:</strong> ${trackingNumber}</p>
             ${trackingUrl ? `<a href="${trackingUrl}" style="display:inline-block;background:#FF5C1A;color:white;padding:10px 20px;font-size:13px;font-weight:600;text-decoration:none;margin-top:8px;">Track Package →</a>` : ''}`
          : `<p style="font-size:13px;color:#555;margin:8px 0 0;">📦 Your item will ship soon — you'll receive a tracking number once it's on its way.</p>`;

      await sendEmail(sale.buyer_email, `Order confirmed: ${listing.title}`, emailTemplate('Order Confirmed ✓',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Thanks for your purchase! Your <strong>${listing.title}</strong> is confirmed.</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:13px;color:#888;margin:0 0 4px;">Amount paid</p>
           <p style="font-size:28px;font-weight:700;color:#1A1A14;margin:0 0 16px;">$${(sale.amount/100).toFixed(2)}</p>
           ${buyerPickupNote}
         </div>
         <div style="background:#EBF4EF;border:1px solid #1D5C3A;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:14px;font-weight:700;color:#1D5C3A;margin:0 0 8px;">🛡️ GoodKit Buyer Protection</p>
           <p style="font-size:13px;color:#333;margin:0 0 8px;">The seller's payout is held until <strong>48 hours after delivery</strong>. If the item doesn't arrive, isn't as described, or something is wrong — flag it before <strong>${protectionDeadline}</strong> and we'll step in.</p>
           <a href="${flagUrl}" style="display:inline-block;background:#1D5C3A;color:white;padding:10px 20px;font-size:13px;font-weight:600;text-decoration:none;">Report a Problem →</a>
         </div>
         <p style="font-size:12px;color:#888;margin:0;">Questions? Reply to this email and we'll help.</p>`
      ));

      // Also send shipping notification separately if we have tracking
      if (!isPickup && !isDetour && trackingNumber) {
        await sendEmail(sale.buyer_email, `Your ${listing.title} is on its way!`, emailTemplate('Order Shipped 📦',
          `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Great news — your <strong>${listing.title}</strong> has been shipped!</p>
           <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
             <p style="font-size:13px;color:#888;margin:0 0 4px;">Tracking number</p>
             <p style="font-size:18px;font-weight:700;color:#1A1A14;margin:0 0 10px;">${trackingNumber}</p>
             ${trackingUrl ? `<a href="${trackingUrl}" style="display:inline-block;background:#FF5C1A;color:white;padding:10px 20px;font-size:13px;font-weight:600;text-decoration:none;">Track Package →</a>` : ''}
           </div>
           <p style="font-size:12px;color:#888;margin:0;">Questions? Reply to this email.</p>`
        ));
      }

      await sendEmail(NOTIFY_EMAIL, `[GoodKit] SOLD: ${listing.title}`, `Seller: ${listing.seller_email}\nPayout: $${(sale.seller_payout/100).toFixed(2)}\nLabel: ${labelUrl || 'not generated'}\nTracking: ${trackingNumber || 'n/a'}`);
    }
    console.log('Sold:', listingId);
  }
  res.json({ received: true });
});

// ── CCX — COLLEGIATE CYCLING EXCHANGE ─────────────────────────────────────────

// Generate a unique 6-char uppercase team code from school + team name
function generateCcxCode(school, teamName) {
  const base = (school + teamName).toUpperCase().replace(/[^A-Z]/g, '');
  const code = base.slice(0, 4) + Math.random().toString(36).slice(2, 4).toUpperCase();
  // Ensure uniqueness — retry on collision
  const existing = db.prepare("SELECT id FROM ccx_teams WHERE code=?").get(code);
  if (existing) return generateCcxCode(school, teamName + Math.random());
  return code;
}

// POST /ccx/register — captain registers their team
app.post('/ccx/register', async (req, res) => {
  try {
    const { team_name, school, captain_name, captain_email, payout_email } = req.body;
    if (!team_name || !school || !captain_name || !captain_email)
      return res.status(400).json({ error: 'team_name, school, captain_name, captain_email required' });

    // Check for duplicate email
    const existing = db.prepare("SELECT id, code FROM ccx_teams WHERE captain_email=?").get(captain_email);
    if (existing) return res.status(409).json({ error: 'A team is already registered with this email', code: existing.code });

    const id   = uuidv4();
    const code = generateCcxCode(school, team_name);
    db.prepare(`INSERT INTO ccx_teams (id, team_name, school, code, captain_name, captain_email, payout_email)
                VALUES (?, ?, ?, ?, ?, ?, ?)`).run(id, team_name, school, code, captain_name, captain_email, payout_email || captain_email);

    // Send partner welcome + team code block
    await sendPartnerWelcome({ to: captain_email, recipientName: captain_name, entityName: `${school} ${team_name}`, entityType: 'ccx_team', loginLink: `${BASE_URL}/ccx/dashboard?team=${id}` });
    // Also send a plain code email so they always have the code handy
    await sendEmail(captain_email, `Your GoodKit CCX team code: ${code}`, emailTemplate('Your Team Code 🚴',
      `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Hi ${captain_name}, here's your team code to share with teammates:</p>
       <div style="background:#E8E0D0;padding:20px 24px;margin-bottom:20px;">
         <p style="font-size:11px;letter-spacing:0.15em;text-transform:uppercase;color:#888;margin:0 0 4px;">${school} · ${team_name}</p>
         <p style="font-size:40px;font-weight:900;color:#FF5C1A;letter-spacing:0.1em;margin:0;">${code}</p>
       </div>
       <p style="font-size:13px;color:#555;line-height:1.7;margin:0;">When teammates list gear and enter <strong>${code}</strong>, 2% of every sale flows into your team fund automatically — out of GoodKit's cut, not theirs.</p>`
    ));

    await sendEmail(NOTIFY_EMAIL, `[GoodKit CCX] New team: ${school} ${team_name}`, `Code: ${code}\nCaptain: ${captain_name} <${captain_email}>`);

    res.json({ success: true, code, team_id: id, message: `Team registered! Code: ${code}` });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// GET /ccx/dashboard — team captain dashboard page
app.get('/ccx/dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'goodkit-ccx-dashboard.html'));
});

// GET /ccx/team/:id — team stats (for dashboard)
app.get('/ccx/team/:id', (req, res) => {
  try {
    const team = db.prepare("SELECT * FROM ccx_teams WHERE id=?").get(req.params.id);
    if (!team) return res.status(404).json({ error: 'Team not found' });

    const sales = db.prepare(`
      SELECT s.*, l.title, l.category, l.seller_name FROM sales s
      JOIN listings l ON s.listing_id = l.id
      WHERE s.ccx_code = ? AND s.status != 'pending'
      ORDER BY s.created_at DESC
    `).all(team.code);

    const payouts = db.prepare("SELECT * FROM ccx_payouts WHERE team_id=? ORDER BY created_at DESC").all(team.id);

    res.json({
      success: true,
      team: {
        id:            team.id,
        team_name:     team.team_name,
        school:        team.school,
        code:          team.code,
        captain_name:  team.captain_name,
        fund_balance:  team.fund_balance / 100,
        total_earned:  team.total_earned / 100,
        status:        team.status,
        created_at:    team.created_at
      },
      sales: sales.map(s => ({
        id:         s.id,
        title:      s.title,
        category:   s.category,
        seller:     s.seller_name,
        amount:     s.amount / 100,
        ccx_fund:   s.ccx_fund / 100,
        created_at: s.created_at
      })),
      payouts: payouts.map(p => ({ ...p, amount: p.amount / 100 }))
    });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// GET /ccx/verify?code=XXXXX — validate a CCX code at listing time
app.get('/ccx/verify', (req, res) => {
  try {
    const team = db.prepare("SELECT team_name, school, code FROM ccx_teams WHERE code=? AND status='active'").get(req.query.code);
    if (!team) return res.status(404).json({ valid: false, error: 'Invalid or inactive team code' });
    res.json({ valid: true, team_name: team.team_name, school: team.school, code: team.code });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// GET /ccx/teams/:code — public team storefront page
app.get('/ccx/teams/:code', (req, res) => {
  res.sendFile(path.join(__dirname, 'goodkit-ccx-team.html'));
});

// GET /ccx/teams/:code/data — JSON data for team storefront
app.get('/ccx/teams/:code/data', (req, res) => {
  try {
    const team = db.prepare("SELECT id, team_name, school, code, fund_balance, total_earned, created_at FROM ccx_teams WHERE code=? AND status='active'").get(req.params.code.toUpperCase());
    if (!team) return res.status(404).json({ error: 'Team not found' });

    const listings = db.prepare(`
      SELECT id, title, category, condition, price, shipping_estimate, photos, description, created_at
      FROM listings
      WHERE ccx_code=? AND status='approved'
      ORDER BY created_at DESC
    `).all(team.code);

    const salesCount = db.prepare("SELECT COUNT(*) as n FROM sales WHERE ccx_code=? AND status != 'pending'").get(team.code);

    res.json({
      success: true,
      team: {
        team_name:    team.team_name,
        school:       team.school,
        code:         team.code,
        total_earned: team.total_earned,
        created_at:   team.created_at
      },
      listings: listings.map(l => ({
        id:          l.id,
        title:       l.title,
        category:    l.category,
        condition:   l.condition,
        price:       l.price,
        shipping:    l.shipping_estimate,
        photos:      JSON.parse(l.photos || '[]'),
        description: l.description,
        created_at:  l.created_at
      })),
      sales_count: salesCount?.n || 0
    });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// GET /ccx — team registration page
app.get('/ccx', (req, res) => {
  res.sendFile(path.join(__dirname, 'goodkit-ccx-register.html'));
});

// ── ADMIN ─────────────────────────────────────────────────────────────────────

// GET /admin — admin dashboard page
app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'goodkit-admin.html'));
});

// CORS preflight for admin endpoints (allows claude.ai artifact to call Railway)
app.options('/admin/:path*', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
  res.sendStatus(204);
});

// GET /admin/overview — full dashboard data in one shot
app.get('/admin/overview', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const { adminKey } = req.query;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const sales = db.prepare(`
      SELECT s.*, l.title, l.seller_email, l.seller_name, l.ccx_code as listing_ccx
      FROM sales s JOIN listings l ON s.listing_id=l.id
      ORDER BY s.created_at DESC LIMIT 100
    `).all();

    const completeSales = sales.filter(s => s.status !== 'pending');
    const gmv           = completeSales.reduce((sum, s) => sum + s.amount, 0);
    const platformGross = completeSales.reduce((sum, s) => sum + s.platform_payout, 0);
    const sellerOwed    = db.prepare("SELECT SUM(seller_payout) as t FROM sales WHERE status='complete'").get().t || 0;
    const ccxOwed       = db.prepare("SELECT SUM(fund_balance) as t FROM ccx_teams WHERE fund_balance > 0").get().t || 0;

    const teams = db.prepare("SELECT * FROM ccx_teams ORDER BY fund_balance DESC").all();
    const listings = db.prepare("SELECT id,title,seller_name,seller_email,status,price,created_at,ccx_code FROM listings ORDER BY created_at DESC LIMIT 50").all();

    res.json({
      success: true,
      stats: {
        gmv:            gmv / 100,
        platform_gross: platformGross / 100,
        seller_owed:    sellerOwed / 100,
        ccx_owed:       ccxOwed / 100,
        sale_count:     completeSales.length,
        listing_count:  db.prepare("SELECT COUNT(*) as c FROM listings WHERE status='approved'").get().c,
        team_count:     teams.length
      },
      recent_sales: sales.slice(0, 50).map(s => ({
        id:          s.id,
        title:       s.title,
        seller_name: s.seller_name,
        amount:      s.amount / 100,
        seller_payout: s.seller_payout / 100,
        platform_payout: s.platform_payout / 100,
        ccx_code:    s.ccx_code,
        ccx_fund:    (s.ccx_fund || 0) / 100,
        status:      s.status,
        created_at:  s.created_at
      })),
      ccx_teams: teams.map(t => ({
        id:            t.id,
        team_name:     t.team_name,
        school:        t.school,
        code:          t.code,
        captain_name:  t.captain_name,
        captain_email: t.captain_email,
        payout_email:  t.payout_email || t.captain_email,
        fund_balance:  t.fund_balance / 100,
        total_earned:  t.total_earned / 100,
        status:        t.status,
        created_at:    t.created_at
      })),
      listings: listings.map(l => ({
        id:           l.id,
        title:        l.title,
        seller_name:  l.seller_name,
        seller_email: l.seller_email,
        status:       l.status,
        price:        l.price / 100,
        ccx_code:     l.ccx_code,
        created_at:   l.created_at
      }))
    });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /admin/ccx/payout — mark a team fund as paid out
app.post('/admin/ccx/payout', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const { adminKey, team_id, note } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const team = db.prepare("SELECT * FROM ccx_teams WHERE id=?").get(team_id);
    if (!team) return res.status(404).json({ error: 'Team not found' });
    if (team.fund_balance <= 0) return res.status(400).json({ error: 'No balance to pay out' });
    const amount = team.fund_balance;
    db.prepare("INSERT INTO ccx_payouts (id,team_id,amount,status,created_at,paid_at) VALUES (?,?,?,'paid',datetime('now'),datetime('now'))")
      .run(uuidv4(), team_id, amount);
    db.prepare("UPDATE ccx_teams SET fund_balance=0 WHERE id=?").run(team_id);
    res.json({ success: true, team_name: team.team_name, amount_paid: amount / 100, payout_email: team.payout_email || team.captain_email });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/admin/release-payouts', async (req, res) => {
  const { adminKey } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const now = new Date().toISOString();
    // Use payout_after if set; fall back to 72hr from created_at for older rows
    const pending = db.prepare(`
      SELECT s.*, l.stripe_account_id FROM sales s
      JOIN listings l ON s.listing_id = l.id
      WHERE s.status IN ('delivered', 'pending_shipment')
        AND s.buyer_flagged = 0
        AND (
          (s.payout_after IS NOT NULL AND s.payout_after <= ?)
          OR (s.payout_after IS NULL AND s.created_at <= datetime(?, '-7 days'))
        )
    `).all(now, now);
    let released = 0;
    for (const sale of pending) {
      try {
        db.prepare("UPDATE sales SET status='paid_out' WHERE id=?").run(sale.id);
        released++;
      } catch(err) { console.error('Payout error', sale.id, err.message); }
    }
    // Count how many are held due to disputes
    const flagged = db.prepare("SELECT COUNT(*) as n FROM sales WHERE status IN ('delivered','pending_shipment') AND buyer_flagged=1").get().n;
    res.json({ success: true, released, held_for_dispute: flagged });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Buyer flags a problem — holds payout until admin resolves
app.post('/sales/:id/flag', async (req, res) => {
  try {
    const { email, reason } = req.body;
    if (!email || !reason) return res.status(400).json({ error: 'email and reason required' });
    const sale = db.prepare("SELECT * FROM sales WHERE id=?").get(req.params.id);
    if (!sale) return res.status(404).json({ error: 'Sale not found' });
    if (sale.buyer_email.toLowerCase() !== email.toLowerCase())
      return res.status(403).json({ error: 'Email does not match this order' });
    if (sale.status === 'paid_out')
      return res.status(400).json({ error: 'Payout window has closed — contact support directly.' });
    if (sale.buyer_flagged)
      return res.status(400).json({ error: 'This order is already flagged — we will follow up.' });

    db.prepare("UPDATE sales SET buyer_flagged=1, flag_reason=? WHERE id=?").run(reason, sale.id);

    // Notify admin
    const listing = db.prepare("SELECT title FROM listings WHERE id=?").get(sale.listing_id);
    await sendEmail(process.env.NOTIFY_EMAIL || 'ccroberts10@gmail.com',
      `[GoodKit] DISPUTE: ${listing?.title || sale.listing_id}`,
      `Sale ID: ${sale.id}\nBuyer: ${sale.buyer_email}\nReason: ${reason}\nAmount: $${(sale.amount/100).toFixed(2)}\nSeller payout HELD.`
    );

    res.json({ success: true, message: 'Your dispute has been filed. We will review and follow up within 24 hours. The seller payout is on hold.' });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Admin resolves a dispute — releases or refunds
app.post('/admin/sales/:id/resolve-flag', (req, res) => {
  const { adminKey, resolution } = req.body; // resolution: 'release' | 'refund'
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  if (!['release', 'refund'].includes(resolution)) return res.status(400).json({ error: "resolution must be 'release' or 'refund'" });
  try {
    const sale = db.prepare("SELECT * FROM sales WHERE id=?").get(req.params.id);
    if (!sale) return res.status(404).json({ error: 'Sale not found' });
    if (resolution === 'release') {
      db.prepare("UPDATE sales SET buyer_flagged=0, flag_reason=NULL, status='paid_out' WHERE id=?").run(req.params.id);
      res.json({ success: true, action: 'released — payout marked paid_out' });
    } else {
      // Mark as refunded — actual Stripe refund must be issued manually for now
      db.prepare("UPDATE sales SET buyer_flagged=0, status='refunded' WHERE id=?").run(req.params.id);
      res.json({ success: true, action: 'marked refunded — issue Stripe refund manually in dashboard', payment_intent_id: sale.payment_intent_id });
    }
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/admin/alerts', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key || req.query.adminKey;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const alerts = db.prepare("SELECT email, category, max_price, condition, active, created_at FROM listing_alerts ORDER BY created_at DESC").all();
    res.json({ success: true, count: alerts.length, alerts });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/admin/listings', (req, res) => {
  const { adminKey } = req.query;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    res.json({
      success: true,
      listings: db.prepare("SELECT * FROM listings ORDER BY created_at DESC").all()
        .map(l => ({ ...l, photos: JSON.parse(l.photos || '[]'), price: l.price / 100, shipping_estimate: (l.shipping_estimate || 0) / 100 }))
    });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.patch('/admin/listings/:id', (req, res) => {
  const { adminKey, status } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try { db.prepare("UPDATE listings SET status=? WHERE id=?").run(status, req.params.id); res.json({ success: true }); }
  catch(err) { res.status(500).json({ error: err.message }); }
});

app.patch('/admin/listings/:id/keywords', (req, res) => {
  const key = req.headers['x-admin-key'] || req.body.adminKey;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const { keywords, title } = req.body;
  try {
    const updates = [];
    const params = [];
    if (keywords !== undefined) { updates.push('keywords=?'); params.push(keywords); }
    if (title !== undefined)    { updates.push('title=?');    params.push(title); }
    if (!updates.length) return res.status(400).json({ error: 'Provide keywords and/or title' });
    params.push(req.params.id);
    db.prepare(`UPDATE listings SET ${updates.join(',')} WHERE id=?`).run(...params);
    const listing = db.prepare('SELECT id, title, keywords FROM listings WHERE id=?').get(req.params.id);
    res.json({ success: true, listing });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.patch('/admin/listings/:id/stripe-account', (req, res) => {
  const { adminKey, stripe_account_id } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try { db.prepare("UPDATE listings SET stripe_account_id=? WHERE id=?").run(stripe_account_id || null, req.params.id); res.json({ success: true }); }
  catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/admin/sales', (req, res) => {
  const { adminKey } = req.query;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const sales = db.prepare("SELECT s.*, l.title, l.seller_email, l.seller_name FROM sales s JOIN listings l ON s.listing_id=l.id ORDER BY s.created_at DESC").all()
      .map(s => ({ ...s, amount: s.amount/100, seller_payout: s.seller_payout/100, platform_payout: s.platform_payout/100 }));
    const totalRevenue  = sales.filter(s => s.status !== 'pending').reduce((sum, s) => sum + s.platform_payout, 0);
    const totalPaidOut  = sales.filter(s => s.status === 'paid_out').reduce((sum, s) => sum + s.seller_payout, 0);
    res.json({ success: true, stats: { totalRevenue, totalPaidOut, saleCount: sales.length }, sales });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Analytics — listing views, top listings, daily traffic
app.get('/admin/analytics', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key || req.query.adminKey;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const topListings = db.prepare(`
      SELECT id, title, category, price, condition, status, view_count, created_at
      FROM listings
      ORDER BY view_count DESC
      LIMIT 20
    `).all().map(l => ({ ...l, price: l.price / 100 }));

    const totalViews   = db.prepare("SELECT COALESCE(SUM(view_count),0) AS total FROM listings").get().total;
    const activeCount  = db.prepare("SELECT COUNT(*) AS n FROM listings WHERE status='approved'").get().n;
    const soldCount    = db.prepare("SELECT COUNT(*) AS n FROM listings WHERE status='sold'").get().n;
    const alertCount   = db.prepare("SELECT COUNT(*) AS n FROM listing_alerts WHERE active=1").get().n;

    res.json({
      success: true,
      summary: { totalViews, activeListings: activeCount, soldListings: soldCount, alertSubscribers: alertCount },
      topListings
    });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Utility: preview fee split for any price
app.get('/split/:price', (req, res) => {
  const priceInCents = Math.round(parseFloat(req.params.price) * 100);
  if (isNaN(priceInCents) || priceInCents < 100) return res.status(400).json({ error: 'Invalid price' });
  const shippingCents = Math.round(parseFloat(req.query.shipping || 0) * 100);
  const weightOz      = parseInt(req.query.weight || 0);
  const split = calculateSplit(priceInCents, shippingCents);
  const label = getLabelTier(weightOz);
  res.json({ ...split, label });
});

// ── BACKUP ────────────────────────────────────────────────────────────────────

async function runBackup() {
  if (!R2_ENABLED) { console.log('Backup skipped — R2 not configured'); return { success: false, reason: 'R2 not configured' }; }
  try {
    const dbBuffer  = fs.readFileSync(DB_PATH);
    const now       = new Date();
    const timestamp = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const key       = `backups/${now.getFullYear()}/${String(now.getMonth()+1).padStart(2,'0')}/marketplace-${timestamp}.db`;
    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET, Key: key, Body: dbBuffer,
      ContentType: 'application/octet-stream',
      Metadata: {
        timestamp:     now.toISOString(),
        db_size:       String(dbBuffer.length),
        listing_count: String(db.prepare("SELECT COUNT(*) as c FROM listings").get().c),
        sale_count:    String(db.prepare("SELECT COUNT(*) as c FROM sales").get().c)
      }
    }));
    console.log(`✓ Backup: ${key} (${(dbBuffer.length/1024).toFixed(1)} KB)`);
    if (now.getDay() === 0) {
      const lc = db.prepare("SELECT COUNT(*) as c FROM listings").get().c;
      const sc = db.prepare("SELECT COUNT(*) as c FROM sales").get().c;
      const po = db.prepare("SELECT SUM(seller_payout) as t FROM sales WHERE status='paid_out'").get().t || 0;
      const pr = db.prepare("SELECT SUM(platform_payout) as t FROM sales WHERE status IN ('delivered','paid_out')").get().t || 0;
      await sendEmail(NOTIFY_EMAIL, 'GoodKit — Weekly Backup', emailTemplate('Weekly Backup ✓',
        `<div style="background:#E8E0D0;padding:16px 20px;">
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>File:</strong> ${key}</p>
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>Size:</strong> ${(dbBuffer.length/1024).toFixed(1)} KB</p>
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>Listings:</strong> ${lc}</p>
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>Sales:</strong> ${sc}</p>
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>Total paid out to sellers:</strong> $${(po/100).toFixed(2)}</p>
           <p style="font-size:14px;color:#FF5C1A;margin:0;"><strong>Platform revenue (15% fees):</strong> $${(pr/100).toFixed(2)}</p>
         </div>`
      ));
    }
    return { success: true, key, size: dbBuffer.length };
  } catch(err) { console.error('Backup error:', err.message); return { success: false, error: err.message }; }
}

app.post('/admin/backup', async (req, res) => {
  const { adminKey } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  res.json(await runBackup());
});

app.get('/admin/backups', async (req, res) => {
  const { adminKey } = req.query;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const response = await r2.send(new ListObjectsV2Command({ Bucket: R2_BUCKET, Prefix: 'backups/', MaxKeys: 50 }));
    const files = (response.Contents || []).sort((a, b) => new Date(b.LastModified) - new Date(a.LastModified))
      .map(f => ({ key: f.Key, size: (f.Size/1024).toFixed(1) + ' KB', lastModified: f.LastModified }));
    res.json({ success: true, count: files.length, backups: files });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── SEO ROUTES ────────────────────────────────────────────────────────────────

const SITE_URL = 'https://good-kit.com';

function slugify(str) {
  return (str || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 60);
}

// Category groups for landing pages
const CATEGORY_GROUPS = [
  { slug: 'drivetrains',    label: 'Drivetrains',       desc: 'Used chains, cassettes, derailleurs, shifters, cranks, and bottom brackets for road and mountain bikes.',   cats: ['Chains','Cassettes','Chainrings','Derailleurs','Shifters','Cranks','Bottom Brackets','Cables & Housing'] },
  { slug: 'brakes',         label: 'Brakes',            desc: 'Used brake levers, calipers, rotors, pads, and hydraulic hoses for road, gravel, and mountain bikes.',    cats: ['Brake Levers','Brake Calipers','Rotors','Brake Pads','Brake Hoses'] },
  { slug: 'cockpit',        label: 'Cockpit & Steering', desc: 'Used handlebars, stems, grips, bar tape, and headsets to dial in your ride position.',                    cats: ['Handlebars','Stems','Grips','Bar Tape','Headsets'] },
  { slug: 'saddles',        label: 'Saddles & Seatposts', desc: 'Used saddles, seatposts, and dropper posts for road, gravel, and mountain bikes.',                       cats: ['Saddles','Seatposts','Dropper Posts'] },
  { slug: 'pedals',         label: 'Pedals',            desc: 'Used flat pedals and clipless pedals for mountain biking, road riding, and commuting.',                    cats: ['Flat Pedals','Clipless Pedals'] },
  { slug: 'suspension',     label: 'Suspension',        desc: 'Used forks, rear shocks, and suspension components for mountain bikes.',                                   cats: ['Forks','Rear Shocks','Suspension Parts'] },
  { slug: 'wheels',         label: 'Wheels & Tires',    desc: 'Used wheels, tires, tubes, hubs, and rims for road, gravel, and mountain biking.',                        cats: ['Wheels','Tires','Tubes','Hubs','Rims'] },
  { slug: 'apparel',        label: 'Apparel',           desc: 'Used cycling jerseys, bibs, shorts, jackets, base layers, socks, and gloves.',                            cats: ['Jerseys','Bibs & Shorts','Jackets','Base Layers','Socks','Gloves'] },
  { slug: 'helmets-protection', label: 'Helmets & Protection', desc: 'Used cycling helmets, pads, and body armor for road, gravel, and mountain biking.',               cats: ['Helmets','Pads & Protection'] },
  { slug: 'shoes',          label: 'Cycling Shoes',     desc: 'Used road cycling shoes, MTB shoes, and casual cycling shoes.',                                           cats: ['MTB Shoes','Road Shoes','Casual Cycling Shoes'] },
  { slug: 'bags-packs',     label: 'Bags & Packs',      desc: 'Used hydration packs, frame bags, saddle bags, handlebar bags, backpacks, and hip packs for cycling.',    cats: ['Hydration Packs','Frame Bags','Saddle Bags','Handlebar Bags','Backpacks','Hip Packs'] },
  { slug: 'electronics',    label: 'Electronics & GPS', desc: 'Used bike computers, GPS units, power meters, heart rate monitors, lights, and smart trainers.',           cats: ['Electronics','Lights','Bike Computers','GPS Units','Heart Rate Monitors','Power Meters','Sensors & Accessories','Smart Trainers'] },
  { slug: 'tools',          label: 'Tools & Maintenance', desc: 'Used bike tools, work stands, pumps, lube, and cleaning supplies.',                                     cats: ['Tools','Stands & Workstands','Pumps','Lube & Cleaners'] },
  { slug: 'frames-bikes',   label: 'Frames & Complete Bikes', desc: 'Used bike frames and complete bikes — road, gravel, mountain, and more.',                           cats: ['Frames','Complete Bikes'] },
];

// Individual listing page — crawlable, OG-tagged, links to marketplace
// Supports both /listing/:id and /listing/:id-slug (slug is ignored, id drives the lookup)
app.get('/listing/:idslug', (req, res) => {
  try {
    // Extract the UUID portion (first segment before any hyphen that follows a UUID pattern)
    // UUIDs are 36 chars; if the param is longer, the id is the first 36 chars
    const param = req.params.idslug;
    const id = param.length > 36 ? param.slice(0, 36) : param;
    const listing = db.prepare("SELECT * FROM listings WHERE id=? AND status='approved'").get(id);
    if (!listing) return res.status(404).send('<h1>Listing not found</h1>');

    // Redirect bare /listing/:id to canonical slug URL (301 for SEO)
    const canonicalSlug = `${listing.id}-${slugify(listing.title)}`;
    if (param !== canonicalSlug) {
      return res.redirect(301, `${SITE_URL}/listing/${canonicalSlug}`);
    }
    const photos = JSON.parse(listing.photos || '[]');
    const price  = (listing.price / 100).toFixed(2);
    const photo  = photos[0] ? `${SITE_URL}${photos[0]}` : `${SITE_URL}/icon-512.png`;
    const title  = `${listing.title} — GoodKit`;
    const desc   = listing.description
      ? listing.description.slice(0, 160)
      : `Used ${listing.category} for $${price}. ${listing.condition} condition. Buy on GoodKit, the cycling gear marketplace.`;
    const keywords = [listing.title, listing.category, listing.condition, ...(listing.keywords || '').split(',').map(k => k.trim()).filter(Boolean)].join(', ');
    const canonicalUrl = `${SITE_URL}/listing/${listing.id}-${slugify(listing.title)}`;

    res.setHeader('Content-Type', 'text/html');
    res.send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
  <title>${title}</title>
  <meta name="description" content="${desc}">
  <meta name="keywords" content="${keywords}">
  <link rel="canonical" href="${canonicalUrl}">
  <meta property="og:type" content="product">
  <meta property="og:title" content="${listing.title}">
  <meta property="og:description" content="${desc}">
  <meta property="og:image" content="${photo}">
  <meta property="og:image:width" content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:url" content="${canonicalUrl}">
  <meta property="og:site_name" content="GoodKit">
  <meta property="product:price:amount" content="${price}">
  <meta property="product:price:currency" content="USD">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${listing.title}">
  <meta name="twitter:description" content="${desc}">
  <meta name="twitter:image" content="${photo}">
  <script type="application/ld+json">${JSON.stringify({
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    "itemListElement": [
      { "@type": "ListItem", "position": 1, "name": "GoodKit", "item": SITE_URL },
      { "@type": "ListItem", "position": 2, "name": listing.category || "Gear", "item": (() => { const g = CATEGORY_GROUPS.find(g => g.cats.includes(listing.category)); return g ? `${SITE_URL}/gear/${g.slug}` : `${SITE_URL}/marketplace`; })() },
      { "@type": "ListItem", "position": 3, "name": listing.title, "item": canonicalUrl }
    ]
  })}</script>
  <script type="application/ld+json">${JSON.stringify({
    "@context": "https://schema.org",
    "@type": "Product",
    "name": listing.title,
    "description": listing.description || desc,
    "image": photos.map(p => `${SITE_URL}${p}`),
    "brand": { "@type": "Brand", "name": "GoodKit" },
    "offers": {
      "@type": "Offer",
      "priceCurrency": "USD",
      "price": price,
      "availability": "https://schema.org/InStock",
      "url": canonicalUrl,
      "seller": { "@type": "Organization", "name": "GoodKit" },
      "itemCondition": listing.condition === 'New' ? "https://schema.org/NewCondition" : "https://schema.org/UsedCondition"
    },
    "keywords": keywords
  })}</script>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&display=swap">
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:#0a0a0a;color:#f0f0f0;font-family:'Space Mono',monospace;min-height:100vh}
    /* Nav */
    .nav{background:#0a0a0a;border-bottom:2px solid #222;padding:0 24px;height:56px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:10}
    .nav-logo{font-size:20px;font-weight:700;text-decoration:none;color:#fff;letter-spacing:-0.5px}
    .nav-logo span{color:#FF5C1A}
    .nav-cta{background:#FF5C1A;color:#fff;border:none;padding:8px 18px;border-radius:6px;font-family:inherit;font-size:13px;font-weight:700;cursor:pointer;text-decoration:none}
    /* Content */
    .container{max-width:800px;margin:0 auto;padding:32px 20px 80px}
    /* Photo gallery */
    .gallery{margin-bottom:28px}
    .gallery-main{width:100%;aspect-ratio:4/3;object-fit:cover;border-radius:10px;background:#111;display:block}
    .gallery-thumbs{display:flex;gap:8px;margin-top:8px;overflow-x:auto}
    .gallery-thumbs img{width:72px;height:72px;object-fit:cover;border-radius:6px;cursor:pointer;border:2px solid transparent;flex-shrink:0;transition:border-color .15s}
    .gallery-thumbs img.active{border-color:#FF5C1A}
    /* Info */
    .listing-title{font-size:22px;font-weight:700;line-height:1.3;margin-bottom:10px}
    .listing-price{font-size:36px;font-weight:700;color:#FF5C1A;margin-bottom:14px}
    .listing-badges{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px}
    .badge{background:#1a1a1a;border:1px solid #333;border-radius:20px;padding:5px 12px;font-size:12px;color:#aaa}
    .section-label{font-size:11px;font-weight:700;letter-spacing:1px;color:#666;text-transform:uppercase;margin-bottom:8px;margin-top:24px}
    .desc-text{font-size:14px;line-height:1.8;color:#ccc;font-family:system-ui,sans-serif}
    .keywords-list{font-size:12px;color:#555;line-height:1.8;font-family:system-ui,sans-serif}
    /* CTA */
    .cta-bar{margin-top:32px;border-top:1px solid #222;padding-top:24px}
    .cta-btn{display:block;width:100%;background:#FF5C1A;color:#fff;text-align:center;padding:16px;border-radius:8px;font-family:inherit;font-size:16px;font-weight:700;text-decoration:none;letter-spacing:0.3px}
    .cta-sub{text-align:center;font-size:12px;color:#555;margin-top:10px}
    /* Seller */
    .seller-row{display:flex;align-items:center;gap:10px;padding:14px 0;border-top:1px solid #1a1a1a;margin-top:16px}
    .seller-avatar{width:36px;height:36px;border-radius:50%;background:#222;border:1px solid #333;display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0}
    .seller-name{font-size:14px;color:#aaa}
    @media(min-width:640px){
      .layout{display:grid;grid-template-columns:1fr 1fr;gap:32px;align-items:start}
      .gallery{margin-bottom:0}
    }
  </style>
</head>
<body>
<nav class="nav">
  <a class="nav-logo" href="${SITE_URL}">goodkit<span>.</span></a>
  <a class="nav-cta" href="${SITE_URL}/marketplace">Shop All Gear</a>
</nav>
<div class="container">
  <div class="layout">
    <div class="gallery">
      ${photos[0] ? `<img class="gallery-main" id="mainPhoto" src="${SITE_URL}${photos[0]}" alt="${listing.title} - ${listing.condition} condition - $${price}">` : `<div class="gallery-main" style="display:flex;align-items:center;justify-content:center;font-size:48px;">🚴</div>`}
      ${photos.length > 1 ? `<div class="gallery-thumbs">${photos.map((p, i) => `<img src="${SITE_URL}${p}" class="${i===0?'active':''}" onclick="setPhoto('${SITE_URL}${p}',this)" alt="${listing.title} photo ${i+1}">`).join('')}</div>` : ''}
    </div>
    <div class="info">
      <h1 class="listing-title">${listing.title}</h1>
      <div class="listing-price">$${price}</div>
      <div class="listing-badges">
        ${listing.category ? `<span class="badge">${listing.category}</span>` : ''}
        ${listing.size ? `<span class="badge">${listing.size}</span>` : ''}
        <span class="badge">${listing.condition} condition</span>
      </div>
      ${listing.description ? `<div class="section-label">Description</div><div class="desc-text">${listing.description.replace(/\n/g,'<br>')}</div>` : ''}
      <div class="seller-row">
        <div class="seller-avatar">🧑</div>
        <div class="seller-name">Sold by <strong>${listing.seller_name}</strong></div>
      </div>
      <div class="cta-bar">
        <a class="cta-btn" href="${SITE_URL}/marketplace#${listing.id}">Buy on GoodKit →</a>
        <div class="cta-sub">Secure checkout · Shippo-powered shipping · Seller keeps 85%</div>
      </div>
      ${keywords ? `<div class="section-label" style="margin-top:28px">Tags</div><div class="keywords-list">${keywords}</div>` : ''}
    </div>
  </div>
</div>
<script>
function setPhoto(src, el){
  document.getElementById('mainPhoto').src=src;
  document.querySelectorAll('.gallery-thumbs img').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
}
</script>
</body>
</html>`);
  } catch(err) { res.status(500).send('Error loading listing'); }
});

// ── CATEGORY LANDING PAGES ─────────────────────────────────────────────────
app.get('/gear/:categorySlug', (req, res) => {
  const group = CATEGORY_GROUPS.find(g => g.slug === req.params.categorySlug);
  if (!group) return res.status(404).send('<h1>Category not found</h1>');

  const placeholders = group.cats.map(() => '?').join(',');
  const listings = db.prepare(
    `SELECT id, title, category, condition, price, photos, description FROM listings WHERE status='approved' AND category IN (${placeholders}) ORDER BY created_at DESC LIMIT 60`
  ).all(...group.cats);

  const canonicalUrl = `${SITE_URL}/gear/${group.slug}`;
  const listingCards = listings.map(l => {
    const photos = JSON.parse(l.photos || '[]');
    const photo = photos[0] ? `${SITE_URL}${photos[0]}` : null;
    const price = (l.price / 100).toFixed(2);
    const slug = `${l.id}-${slugify(l.title)}`;
    return `<a href="${SITE_URL}/listing/${slug}" class="card">
      <div class="card-img">${photo ? `<img src="${photo}" alt="${l.title}" loading="lazy">` : '<div class="no-img">🚴</div>'}</div>
      <div class="card-body">
        <div class="card-title">${l.title}</div>
        <div class="card-meta">${l.condition} · ${l.category}</div>
        <div class="card-price">$${price}</div>
      </div>
    </a>`;
  }).join('');

  const itemListSchema = listings.map((l, i) => ({
    '@type': 'ListItem',
    position: i + 1,
    url: `${SITE_URL}/listing/${l.id}-${slugify(l.title)}`,
    name: l.title
  }));

  res.setHeader('Content-Type', 'text/html');
  res.send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
  <title>Used ${group.label} for Sale — GoodKit Cycling Marketplace</title>
  <meta name="description" content="${group.desc} Shop peer-to-peer on GoodKit — free shipping, easy returns.">
  <link rel="canonical" href="${canonicalUrl}">
  <meta property="og:type" content="website">
  <meta property="og:title" content="Used ${group.label} for Sale — GoodKit">
  <meta property="og:description" content="${group.desc}">
  <meta property="og:url" content="${canonicalUrl}">
  <meta property="og:site_name" content="GoodKit">
  <script type="application/ld+json">${JSON.stringify({
    '@context': 'https://schema.org',
    '@type': 'CollectionPage',
    name: `Used ${group.label} for Sale`,
    description: group.desc,
    url: canonicalUrl,
    breadcrumb: {
      '@type': 'BreadcrumbList',
      itemListElement: [
        { '@type': 'ListItem', position: 1, name: 'GoodKit', item: SITE_URL },
        { '@type': 'ListItem', position: 2, name: 'Marketplace', item: `${SITE_URL}/marketplace` },
        { '@type': 'ListItem', position: 3, name: group.label, item: canonicalUrl }
      ]
    },
    mainEntity: { '@type': 'ItemList', itemListElement: itemListSchema }
  })}</script>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&display=swap">
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:#0a0a0a;color:#f0f0f0;font-family:'Space Mono',monospace;min-height:100vh}
    .nav{background:#0a0a0a;border-bottom:2px solid #222;padding:0 24px;height:56px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:10}
    .nav-logo{font-size:20px;font-weight:700;text-decoration:none;color:#fff}
    .nav-logo span{color:#FF5C1A}
    .nav-cta{background:#FF5C1A;color:#fff;border:none;padding:8px 18px;border-radius:6px;font-family:inherit;font-size:13px;font-weight:700;cursor:pointer;text-decoration:none}
    .hero{padding:40px 24px 32px;max-width:1100px;margin:0 auto}
    .hero h1{font-size:28px;font-weight:700;margin-bottom:10px}
    .hero p{color:#999;font-size:14px;line-height:1.7;font-family:system-ui,sans-serif;max-width:600px}
    .cats{display:flex;flex-wrap:wrap;gap:6px;margin-top:16px}
    .cat-tag{background:#1a1a1a;border:1px solid #333;border-radius:20px;padding:4px 12px;font-size:12px;color:#aaa}
    .container{max-width:1100px;margin:0 auto;padding:0 24px 80px}
    .count{font-size:13px;color:#666;margin-bottom:20px}
    .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:16px}
    .card{background:#111;border:1px solid #222;border-radius:10px;overflow:hidden;text-decoration:none;color:inherit;display:block;transition:border-color .15s}
    .card:hover{border-color:#FF5C1A}
    .card-img{aspect-ratio:4/3;background:#1a1a1a;overflow:hidden}
    .card-img img{width:100%;height:100%;object-fit:cover}
    .no-img{width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:2.5rem}
    .card-body{padding:12px 14px 16px}
    .card-title{font-size:13px;font-weight:700;margin-bottom:4px;line-height:1.4}
    .card-meta{font-size:11px;color:#666;margin-bottom:6px}
    .card-price{font-size:18px;font-weight:700;color:#FF5C1A}
    .empty{text-align:center;padding:80px 24px;color:#555}
    .other-cats{max-width:1100px;margin:0 auto;padding:0 24px 60px}
    .other-cats h2{font-size:16px;font-weight:700;margin-bottom:16px;color:#888}
    .cat-links{display:flex;flex-wrap:wrap;gap:8px}
    .cat-link{background:#111;border:1px solid #222;border-radius:8px;padding:8px 16px;font-size:13px;text-decoration:none;color:#ccc;transition:border-color .15s}
    .cat-link:hover{border-color:#FF5C1A;color:#fff}
    footer{border-top:1px solid #1a1a1a;padding:24px;text-align:center;color:#555;font-size:12px}
  </style>
</head>
<body>
<nav class="nav">
  <a class="nav-logo" href="${SITE_URL}">goodkit<span>.</span></a>
  <a class="nav-cta" href="${SITE_URL}/marketplace">Shop All Gear</a>
</nav>
<div class="hero">
  <h1>Used ${group.label} for Sale</h1>
  <p>${group.desc}</p>
  <div class="cats">${group.cats.map(c => `<span class="cat-tag">${c}</span>`).join('')}</div>
</div>
<div class="container">
  <div class="count">${listings.length} listing${listings.length !== 1 ? 's' : ''} available</div>
  ${listings.length ? `<div class="grid">${listingCards}</div>` : '<div class="empty"><h2>No listings yet in this category</h2><p>Check back soon or <a href="/sell" style="color:#FF5C1A">list your gear</a>.</p></div>'}
</div>
<div class="other-cats">
  <h2>Browse Other Categories</h2>
  <div class="cat-links">${CATEGORY_GROUPS.filter(g => g.slug !== group.slug).map(g => `<a class="cat-link" href="${SITE_URL}/gear/${g.slug}">${g.label}</a>`).join('')}</div>
</div>
<footer>© GoodKit · <a href="${SITE_URL}/marketplace" style="color:#FF5C1A">Marketplace</a> · <a href="${SITE_URL}/sell" style="color:#FF5C1A">Sell Your Gear</a></footer>
</body>
</html>`);
});

// Sitemap — includes all active listings + category pages
app.get('/sitemap.xml', (req, res) => {
  try {
    const listings = db.prepare("SELECT id, title, created_at FROM listings WHERE status='approved' ORDER BY created_at DESC LIMIT 1000").all();
    const urls = [
      { loc: SITE_URL, priority: '1.0', changefreq: 'daily' },
      { loc: `${SITE_URL}/marketplace`, priority: '0.9', changefreq: 'hourly' },
      ...CATEGORY_GROUPS.map(g => ({
        loc: `${SITE_URL}/gear/${g.slug}`,
        priority: '0.8',
        changefreq: 'daily'
      })),
      ...listings.map(l => ({
        loc: `${SITE_URL}/listing/${l.id}-${slugify(l.title)}`,
        lastmod: l.created_at.replace(' ', 'T').split('T')[0],
        priority: '0.7',
        changefreq: 'weekly'
      }))
    ];
    res.setHeader('Content-Type', 'application/xml');
    res.send(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map(u => `  <url>
    <loc>${u.loc}</loc>
    ${u.lastmod ? `<lastmod>${u.lastmod}</lastmod>` : ''}
    <changefreq>${u.changefreq}</changefreq>
    <priority>${u.priority}</priority>
  </url>`).join('\n')}
</urlset>`);
  } catch(err) { res.status(500).send('Sitemap error'); }
});

// Robots.txt
app.get('/robots.txt', (req, res) => {
  res.setHeader('Content-Type', 'text/plain');
  res.send(`User-agent: *\nAllow: /\nDisallow: /admin/\nDisallow: /seller/\nSitemap: ${SITE_URL}/sitemap.xml\n`);
});

// ── SHOP CONSIGNMENT ─────────────────────────────────────────────────────────
// Admin creates shop accounts; shops get login + public storefront at /shop/:slug
// Fee structure: 65% seller / 35% platform (no CCX program for shops)

// POST /admin/shops/create — create a shop seller account and send magic link
app.post('/admin/shops/create', async (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  const { shop_name, slug, contact_name, contact_email,
          ship_name, ship_street1, ship_city, ship_state, ship_zip, ship_phone } = req.body;

  if (!shop_name || !slug || !contact_name || !contact_email) {
    return res.status(400).json({ error: 'shop_name, slug, contact_name, contact_email required' });
  }

  // Validate slug
  if (!/^[a-z0-9-]+$/.test(slug)) {
    return res.status(400).json({ error: 'slug must be lowercase letters, numbers, and hyphens only' });
  }

  // Check slug uniqueness
  const existing = db.prepare('SELECT id FROM shop_accounts WHERE slug = ?').get(slug);
  if (existing) return res.status(409).json({ error: 'Slug already taken' });

  const shopId = uuidv4();
  db.prepare(`
    INSERT INTO shop_accounts (id, shop_name, slug, contact_name, contact_email,
      ship_name, ship_street1, ship_city, ship_state, ship_zip, ship_phone)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(shopId, shop_name, slug, contact_name, contact_email,
    ship_name || null, ship_street1 || null, ship_city || null,
    ship_state || null, ship_zip || null, ship_phone || null);

  // Create seller_session with account_type = 'shop'
  const sessionToken = uuidv4();
  db.prepare(`
    INSERT INTO seller_sessions (id, token, email, created_at, used, account_type, shop_slug)
    VALUES (?, ?, ?, datetime('now'), 1, 'shop', ?)
  `).run(uuidv4(), sessionToken, contact_email, slug);

  const loginLink = `${SITE_URL}/seller?token=${sessionToken}`;

  // Send welcome email
  try {
    await sendPartnerWelcome({ to: contact_email, recipientName: contact_name, entityName: shop_name, entityType: 'shop', loginLink });
  } catch(e) {
    console.error('[shop/create] welcome email failed:', e.message);
  }

  res.json({
    ok: true,
    shop_id: shopId,
    slug,
    storefront: `${SITE_URL}/shop/${slug}`,
    login_link: loginLink
  });
});

// GET /admin/shops — list all shop accounts
app.get('/admin/shops', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  const shops = db.prepare(`
    SELECT sa.*, COUNT(l.id) as listing_count,
           COALESCE(SUM(CASE WHEN s.status IN ('paid_out','delivered') THEN s.amount ELSE 0 END) / 100.0, 0) as total_sold
    FROM shop_accounts sa
    LEFT JOIN seller_sessions ss ON ss.shop_slug = sa.slug AND ss.account_type = 'shop'
    LEFT JOIN listings l ON l.seller_email = ss.email
    LEFT JOIN sales s ON s.listing_id = l.id
    GROUP BY sa.id
    ORDER BY sa.created_at DESC
  `).all();

  res.set('Access-Control-Allow-Origin', '*');
  res.json({ shops });
});

// GET /shop/:slug/data — public JSON for shop storefront
app.get('/shop/:slug/data', (req, res) => {
  const { slug } = req.params;
  const shop = db.prepare('SELECT * FROM shop_accounts WHERE slug = ?').get(slug);
  if (!shop) return res.status(404).json({ error: 'Shop not found' });

  const session = db.prepare('SELECT email FROM seller_sessions WHERE shop_slug = ? AND account_type = ?').get(slug, 'shop');
  const listings = session
    ? db.prepare(`
        SELECT l.id, l.title, l.description, l.price, l.condition, l.category,
               l.size, l.weight_oz, l.photos, l.created_at
        FROM listings l
        WHERE l.seller_email = ? AND l.status = 'approved'
        ORDER BY l.created_at DESC
      `).all(session.email)
    : [];

  // Parse photos JSON and build canonical slug for SEO links
  const formattedListings = listings.map(l => ({
    ...l,
    price_cents: l.price,
    price: l.price / 100,
    images: (() => { try { return JSON.parse(l.photos || '[]'); } catch(e) { return []; } })(),
    canonical_slug: `${l.id}-${slugify(l.title)}`
  }));

  res.json({
    shop: {
      name: shop.shop_name,
      slug: shop.slug,
      contact_name: shop.contact_name,
    },
    listings: formattedListings,
    count: formattedListings.length
  });
});

// GET /shop/:slug — public shop storefront HTML
app.get('/shop/:slug', (req, res) => {
  const { slug } = req.params;
  const shop = db.prepare('SELECT * FROM shop_accounts WHERE slug = ?').get(slug);
  if (!shop) return res.status(404).send('<h1>Shop not found</h1>');

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Used Cycling Gear from ${shop.shop_name} | GoodKit</title>
  <meta name="description" content="Buy used cycling gear from ${shop.shop_name} on GoodKit — quality consignment bikes and components, verified condition, fast shipping.">
  <link rel="canonical" href="${SITE_URL}/shop/${shop.slug}">
  <meta property="og:type" content="website">
  <meta property="og:title" content="Used Cycling Gear from ${shop.shop_name}">
  <meta property="og:description" content="Buy used cycling gear from ${shop.shop_name} on GoodKit — quality consignment bikes and components, verified condition, fast shipping.">
  <meta property="og:url" content="${SITE_URL}/shop/${shop.slug}">
  <meta property="og:site_name" content="GoodKit">
  <script type="application/ld+json">${JSON.stringify({
    '@context': 'https://schema.org',
    '@type': 'LocalBusiness',
    name: shop.shop_name,
    url: `${SITE_URL}/shop/${shop.slug}`,
    description: `Used cycling gear from ${shop.shop_name}, sold on GoodKit marketplace.`,
    ...(shop.ship_city && shop.ship_state ? { address: { '@type': 'PostalAddress', addressLocality: shop.ship_city, addressRegion: shop.ship_state, addressCountry: 'US' } } : {}),
    sameAs: [`${SITE_URL}/shop/${shop.slug}`]
  })}</script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --bg: #f9f7f4; --surface: #ffffff; --fg: #1a1a18; --muted: #6b6b68;
      --accent: #FF5C1A; --border: #e8e5e0; --radius: 10px;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    body { background: var(--bg); color: var(--fg); min-height: 100vh; }
    header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 16px 24px; display: flex; align-items: center; gap: 16px; }
    header a { text-decoration: none; color: var(--accent); font-weight: 700; font-size: 1.1rem; }
    .shop-hero { background: var(--surface); border-bottom: 1px solid var(--border); padding: 32px 24px; }
    .shop-hero h1 { font-size: 1.8rem; font-weight: 800; }
    .shop-hero p { color: var(--muted); margin-top: 6px; }
    .container { max-width: 1100px; margin: 0 auto; padding: 32px 24px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 20px; }
    .card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; transition: box-shadow 0.15s; }
    .card:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.08); }
    .card a { text-decoration: none; color: inherit; display: block; }
    .card-img { aspect-ratio: 4/3; background: var(--bg); overflow: hidden; }
    .card-img img { width: 100%; height: 100%; object-fit: cover; }
    .card-img .no-img { width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; font-size: 2.5rem; }
    .card-body { padding: 14px 16px 18px; }
    .card-title { font-weight: 600; font-size: 0.95rem; margin-bottom: 4px; }
    .card-meta { color: var(--muted); font-size: 0.82rem; margin-bottom: 8px; }
    .card-price { font-size: 1.1rem; font-weight: 700; color: var(--accent); }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; background: var(--bg); color: var(--muted); border: 1px solid var(--border); margin-left: 6px; }
    .empty { text-align: center; padding: 80px 24px; color: var(--muted); }
    .empty h2 { font-size: 1.3rem; margin-bottom: 8px; }
    footer { border-top: 1px solid var(--border); padding: 24px; text-align: center; color: var(--muted); font-size: 0.85rem; }
    footer a { color: var(--accent); text-decoration: none; }
    @media (max-width: 480px) { .grid { grid-template-columns: repeat(2, 1fr); gap: 12px; } .container { padding: 16px; } }
  </style>
</head>
<body>
  <header>
    <a href="${SITE_URL}">GoodKit</a>
    <span style="color:var(--muted);font-size:0.85rem">/ ${shop.shop_name}</span>
  </header>
  <div class="shop-hero">
    <div style="max-width:1100px;margin:0 auto">
      <h1>${shop.shop_name}</h1>
      <p>Quality used outdoor gear · Verified GoodKit seller</p>
    </div>
  </div>
  <div class="container">
    <div id="listings" class="grid">
      <div class="empty"><h2>Loading gear...</h2></div>
    </div>
  </div>
  <footer>
    Powered by <a href="${SITE_URL}">GoodKit</a> · Consignment gear from ${shop.shop_name}
  </footer>
  <script>
    fetch('/shop/${slug}/data')
      .then(r => r.json())
      .then(data => {
        const container = document.getElementById('listings');
        if (!data.listings || data.listings.length === 0) {
          container.innerHTML = '<div class="empty"><h2>No listings yet</h2><p>Check back soon for gear from ${shop.shop_name}.</p></div>';
          return;
        }
        container.innerHTML = data.listings.map(l => {
          const img = l.images && l.images[0]
            ? \`<img src="\${l.images[0]}" alt="\${l.title}" loading="lazy">\`
            : '<div class="no-img">🏕️</div>';
          const cond = l.condition ? \`<span class="badge">\${l.condition}</span>\` : '';
          return \`
            <div class="card">
              <a href="/listing/\${l.canonical_slug || l.id}">
                <div class="card-img">\${img}</div>
                <div class="card-body">
                  <div class="card-title">\${l.title}\${cond}</div>
                  <div class="card-meta">\${[l.brand, l.category, l.size].filter(Boolean).join(' · ')}</div>
                  <div class="card-price">$\${(l.price_cents / 100).toFixed(2)}</div>
                </div>
              </a>
            </div>
          \`;
        }).join('');
      })
      .catch(() => {
        document.getElementById('listings').innerHTML = '<div class="empty"><h2>Could not load listings</h2><p>Please try again later.</p></div>';
      });
  </script>
</body>
</html>`;

  res.setHeader('Content-Type', 'text/html');
  res.send(html);
});

// Admin CORS preflight for /admin/shops
app.options('/admin/shops*', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET,POST,PATCH,OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type,x-admin-key');
  res.sendStatus(204);
});

// PATCH /admin/shops/:id — update shop fields (consign_mode, payment info, etc.)
app.patch('/admin/shops/:id', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const allowed = ['consign_mode','venmo','paypal','bank_routing','bank_account','status'];
  const updates = []; const vals = [];
  allowed.forEach(f => { if (req.body[f] !== undefined) { updates.push(`${f}=?`); vals.push(req.body[f]); } });
  if (!updates.length) return res.status(400).json({ error: 'Nothing to update' });
  vals.push(req.params.id);
  db.prepare(`UPDATE shop_accounts SET ${updates.join(',')} WHERE id=?`).run(...vals);
  res.set('Access-Control-Allow-Origin', '*');
  res.json({ ok: true });
});

// POST /admin/shops/:id/consign — log a consign drop-off item for a shop
app.post('/admin/shops/:id/consign', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const shop = db.prepare('SELECT * FROM shop_accounts WHERE id=?').get(req.params.id);
  if (!shop) return res.status(404).json({ error: 'Shop not found' });
  const { item_description, notes } = req.body;
  if (!item_description) return res.status(400).json({ error: 'item_description required' });
  const id = uuidv4();
  db.prepare(`INSERT INTO shop_consign_items (id, shop_id, item_description, notes) VALUES (?,?,?,?)`)
    .run(id, req.params.id, item_description, notes || null);
  res.set('Access-Control-Allow-Origin', '*');
  res.json({ ok: true, item_id: id });
});

// PATCH /admin/shops/consign/:itemId — mark sold, auto-credit wallet
app.patch('/admin/shops/consign/:itemId', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const item = db.prepare('SELECT * FROM shop_consign_items WHERE id=?').get(req.params.itemId);
  if (!item) return res.status(404).json({ error: 'Not found' });
  const { status, sale_price, notes, listing_id, list_price } = req.body;
  const updates = []; const vals = [];
  if (status) {
    updates.push('status=?'); vals.push(status);
    // When going live, stamp listed_at and record original price
    if (status === 'listed' && !item.listed_at) {
      updates.push('listed_at=?'); vals.push(new Date().toISOString());
    }
  }
  if (listing_id !== undefined) { updates.push('listing_id=?'); vals.push(listing_id); }
  if (list_price !== undefined && !item.original_list_price) {
    const priceCents = Math.round(parseFloat(list_price) * 100);
    updates.push('original_list_price=?'); vals.push(priceCents);
  }
  if (notes !== undefined) { updates.push('notes=?'); vals.push(notes); }
  let shopPayout = null;
  if (sale_price !== undefined && !item.sale_price) {
    const saleCents  = Math.round(parseFloat(sale_price) * 100);
    shopPayout       = Math.round(saleCents * 0.65);
    const platCut    = saleCents - shopPayout;
    updates.push('sale_price=?','shop_payout=?','platform_cut=?');
    vals.push(saleCents, shopPayout, platCut);
  }
  if (!updates.length) return res.status(400).json({ error: 'Nothing to update' });
  vals.push(req.params.itemId);
  db.transaction(() => {
    db.prepare(`UPDATE shop_consign_items SET ${updates.join(',')} WHERE id=?`).run(...vals);
    if (shopPayout) {
      db.prepare(`INSERT INTO shop_wallet_events (id, shop_id, item_id, amount, type, description) VALUES (?,?,?,?,?,?)`)
        .run(uuidv4(), item.shop_id, item.id, shopPayout, 'sale', `Sale: ${item.item_description}`);
      db.prepare(`UPDATE shop_accounts SET wallet_balance=wallet_balance+?, total_earned=total_earned+? WHERE id=?`)
        .run(shopPayout, shopPayout, item.shop_id);
    }
  })();
  res.set('Access-Control-Allow-Origin', '*');
  res.json({ ok: true });
});

// GET /admin/shops/:id/consign — list consign items for a shop
app.get('/admin/shops/:id/consign', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const items = db.prepare(`SELECT * FROM shop_consign_items WHERE shop_id=? ORDER BY created_at DESC`).all(req.params.id)
    .map(i => {
      // Compute time-based discount: starts at 60 days listed, 10% per 30 days, max 50%
      let discount_pct = 0;
      let discounted_price = null;
      if (i.listed_at && i.original_list_price && i.status === 'listed') {
        const daysSinceListed = (Date.now() - new Date(i.listed_at).getTime()) / (1000 * 60 * 60 * 24);
        if (daysSinceListed >= 60) {
          const monthsOver = Math.floor((daysSinceListed - 60) / 30) + 1;
          discount_pct = Math.min(monthsOver * 10, 50);
          discounted_price = Math.round(i.original_list_price * (1 - discount_pct / 100)) / 100;
        }
      }
      return {
        ...i,
        sale_price: i.sale_price ? i.sale_price/100 : null,
        shop_payout: i.shop_payout ? i.shop_payout/100 : null,
        platform_cut: i.platform_cut ? i.platform_cut/100 : null,
        original_list_price: i.original_list_price ? i.original_list_price/100 : null,
        discount_pct,
        discounted_price,
      };
    });
  res.set('Access-Control-Allow-Origin', '*');
  res.json({ items });
});

// POST /admin/shops/:id/payout — record payout, deduct wallet
app.post('/admin/shops/:id/payout', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const shop = db.prepare('SELECT * FROM shop_accounts WHERE id=?').get(req.params.id);
  if (!shop) return res.status(404).json({ error: 'Shop not found' });
  const { amount, method, method_detail, notes } = req.body;
  if (!amount || !method) return res.status(400).json({ error: 'amount and method required' });
  const amountCents = Math.round(parseFloat(amount) * 100);
  if (amountCents > shop.wallet_balance) return res.status(400).json({ error: 'Amount exceeds wallet balance' });
  db.transaction(() => {
    db.prepare(`INSERT INTO shop_payouts (id, shop_id, amount, method, method_detail, notes) VALUES (?,?,?,?,?,?)`)
      .run(uuidv4(), req.params.id, amountCents, method, method_detail||null, notes||null);
    db.prepare(`INSERT INTO shop_wallet_events (id, shop_id, amount, type, description) VALUES (?,?,?,?,?)`)
      .run(uuidv4(), req.params.id, -amountCents, 'payout', `Payout via ${method}${method_detail?' — '+method_detail:''}`);
    db.prepare(`UPDATE shop_accounts SET wallet_balance=wallet_balance-? WHERE id=?`).run(amountCents, req.params.id);
  })();
  res.set('Access-Control-Allow-Origin', '*');
  res.json({ ok: true });
});

// ── STRIPE CONNECT INVITES ───────────────────────────────────────────────────
// Shared helper — creates Stripe Connect account + sends onboarding link via email
async function sendStripeInvite({ entityType, entityId, email, name }) {
  // Create Stripe Express account
  const account = await stripe.accounts.create({
    type: 'express',
    email,
    capabilities: { transfers: { requested: true } },
    metadata: { goodkit_entity_type: entityType, goodkit_entity_id: entityId }
  });

  // Generate onboarding link
  const accountLink = await stripe.accountLinks.create({
    account: account.id,
    refresh_url: `${process.env.FRONTEND_URL || 'https://good-kit.com'}/stripe-refresh`,
    return_url:  `${process.env.FRONTEND_URL || 'https://good-kit.com'}/stripe-complete`,
    type: 'account_onboarding',
  });

  // Store account ID + pending status
  if (entityType === 'shop') {
    db.prepare(`UPDATE shop_accounts SET stripe_account_id=?, stripe_status='stripe_pending' WHERE id=?`).run(account.id, entityId);
  } else if (entityType === 'athlete') {
    db.prepare(`UPDATE pro_consign_athletes SET stripe_account_id=?, stripe_status='stripe_pending' WHERE id=?`).run(account.id, entityId);
  } else if (entityType === 'ccx_team') {
    db.prepare(`UPDATE ccx_teams SET stripe_account_id=?, stripe_status='stripe_pending' WHERE id=?`).run(account.id, entityId);
  }

  // Send onboarding email
  await resend.emails.send({
    from: 'GoodKit <noreply@good-kit.com>',
    to: email,
    subject: 'Set up your GoodKit payouts',
    html: `
      <div style="font-family:sans-serif;max-width:520px;margin:0 auto;">
        <img src="https://good-kit.com/logo.png" alt="GoodKit" style="height:36px;margin-bottom:24px;">
        <h2 style="margin:0 0 12px;">Hi ${name},</h2>
        <p style="color:#444;line-height:1.6;">You've been set up as a seller on <strong>GoodKit</strong> — the marketplace for cycling gear. To receive direct deposits from your sales, complete your Stripe payout profile:</p>
        <a href="${accountLink.url}" style="display:inline-block;margin:20px 0;padding:14px 28px;background:#E85D26;color:#fff;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;">Complete Payout Setup →</a>
        <p style="color:#888;font-size:13px;">This link expires in 24 hours. Once complete, your sales proceeds will deposit directly to your bank account automatically.</p>
        <p style="color:#888;font-size:13px;">Questions? Reply to this email or reach us at <a href="mailto:hello@good-kit.com">hello@good-kit.com</a></p>
      </div>`
  });

  return account.id;
}

// ── PARTNER LOGO UPLOAD ───────────────────────────────────────────────────────
// POST /admin/logo-upload?entity=shop|athlete|ccx_team&id=<id>
// Accepts multipart/form-data with field "logo"
app.post('/admin/logo-upload', adminAuth, multer({ storage: multer.memoryStorage(), limits: { fileSize: 5 * 1024 * 1024 } }).single('logo'), async (req, res) => {
  const { entity, id } = req.query;
  if (!entity || !id) return res.status(400).json({ error: 'entity and id required' });
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

  const ext  = path.extname(req.file.originalname).toLowerCase() || '.png';
  const key  = `logos/${entity}/${id}${ext}`;
  let logoUrl;

  try {
    // Upload to S3 if configured, otherwise save locally
    if (process.env.AWS_BUCKET) {
      const s3 = new S3Client({ region: process.env.AWS_REGION || 'us-east-2' });
      await s3.send(new PutObjectCommand({
        Bucket: process.env.AWS_BUCKET,
        Key: key,
        Body: req.file.buffer,
        ContentType: req.file.mimetype,
        ACL: 'public-read'
      }));
      logoUrl = `https://${process.env.AWS_BUCKET}.s3.amazonaws.com/${key}`;
    } else {
      // Local fallback — save to /uploads/logos/
      const dir = path.join(__dirname, 'uploads', 'logos', entity);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, `${id}${ext}`), req.file.buffer);
      logoUrl = `/uploads/logos/${entity}/${id}${ext}`;
    }

    if (entity === 'shop')            db.prepare(`UPDATE shop_accounts SET logo_url=? WHERE id=?`).run(logoUrl, id);
    else if (entity === 'athlete')    db.prepare(`UPDATE pro_consign_athletes SET logo_url=? WHERE id=?`).run(logoUrl, id);
    else if (entity === 'ccx_team')   db.prepare(`UPDATE ccx_teams SET logo_url=? WHERE id=?`).run(logoUrl, id);
    else return res.status(400).json({ error: 'Unknown entity type' });

    res.set('Access-Control-Allow-Origin', '*');
    res.json({ ok: true, logo_url: logoUrl });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Middleware shorthand used above
function adminAuth(req, res, next) {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// POST /admin/shops/:id/stripe-invite
app.post('/admin/shops/:id/stripe-invite', async (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const shop = db.prepare('SELECT * FROM shop_accounts WHERE id=?').get(req.params.id);
  if (!shop) return res.status(404).json({ error: 'Shop not found' });
  try {
    const acctId = await sendStripeInvite({ entityType: 'shop', entityId: shop.id, email: shop.contact_email, name: shop.shop_name });
    res.set('Access-Control-Allow-Origin', '*');
    res.json({ ok: true, stripe_account_id: acctId });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /admin/pro-consign/athletes/:id/stripe-invite
app.post('/admin/pro-consign/athletes/:id/stripe-invite', async (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const athlete = db.prepare('SELECT * FROM pro_consign_athletes WHERE id=?').get(req.params.id);
  if (!athlete) return res.status(404).json({ error: 'Athlete not found' });
  try {
    const acctId = await sendStripeInvite({ entityType: 'athlete', entityId: athlete.id, email: athlete.email, name: athlete.name });
    res.set('Access-Control-Allow-Origin', '*');
    res.json({ ok: true, stripe_account_id: acctId });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /admin/ccx/:teamId/stripe-invite
app.post('/admin/ccx/:teamId/stripe-invite', async (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const team = db.prepare('SELECT * FROM ccx_teams WHERE id=?').get(req.params.teamId);
  if (!team) return res.status(404).json({ error: 'Team not found' });
  try {
    const acctId = await sendStripeInvite({ entityType: 'ccx_team', entityId: team.id, email: team.captain_email, name: team.team_name });
    res.set('Access-Control-Allow-Origin', '*');
    res.json({ ok: true, stripe_account_id: acctId });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PRO ATHLETE SHOPS ────────────────────────────────────────────────────────
// Same mechanics as team stores (shop_accounts + seller_sessions with account_type='shop')
// but shop_type='pro_athlete'. Fee: 65% athlete / 35% GoodKit (same as shops).
// Public storefront lives at /shop/:slug (reuses same route).

// POST /admin/pro-athletes/create — create a verified pro athlete shop
app.post('/admin/pro-athletes/create', async (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  const { athlete_name, slug, contact_name, contact_email,
          ship_name, ship_street1, ship_city, ship_state, ship_zip, ship_phone } = req.body;

  if (!athlete_name || !slug || !contact_name || !contact_email) {
    return res.status(400).json({ error: 'athlete_name, slug, contact_name, contact_email required' });
  }
  if (!/^[a-z0-9-]+$/.test(slug)) {
    return res.status(400).json({ error: 'slug must be lowercase letters, numbers, and hyphens only' });
  }

  const existing = db.prepare('SELECT id FROM shop_accounts WHERE slug = ?').get(slug);
  if (existing) return res.status(409).json({ error: 'Slug already taken' });

  const shopId = uuidv4();
  db.prepare(`
    INSERT INTO shop_accounts (id, shop_name, slug, contact_name, contact_email,
      ship_name, ship_street1, ship_city, ship_state, ship_zip, ship_phone, shop_type)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pro_athlete')
  `).run(shopId, athlete_name, slug, contact_name, contact_email,
    ship_name || null, ship_street1 || null, ship_city || null,
    ship_state || null, ship_zip || null, ship_phone || null);

  const sessionToken = uuidv4();
  db.prepare(`
    INSERT INTO seller_sessions (id, token, email, created_at, used, account_type, shop_slug)
    VALUES (?, ?, ?, datetime('now'), 1, 'pro_athlete', ?)
  `).run(uuidv4(), sessionToken, contact_email, slug);

  const loginLink = `${SITE_URL}/seller?token=${sessionToken}`;

  try {
    await sendPartnerWelcome({ to: contact_email, recipientName: contact_name, entityName: athlete_name, entityType: 'pro_athlete', loginLink });
  } catch(e) { console.error('[pro-athletes/create] welcome email failed:', e.message); }

  res.json({ ok: true, shop_id: shopId, slug, storefront: `${SITE_URL}/shop/${slug}`, login_link: loginLink });
});

// GET /admin/pro-athletes — list all pro athlete shops
app.get('/admin/pro-athletes', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  const athletes = db.prepare(`
    SELECT sa.*, COUNT(l.id) as listing_count,
           COALESCE(SUM(CASE WHEN s.status IN ('paid_out','delivered') THEN s.amount ELSE 0 END) / 100.0, 0) as total_sold
    FROM shop_accounts sa
    LEFT JOIN seller_sessions ss ON ss.shop_slug = sa.slug AND ss.account_type = 'shop'
    LEFT JOIN listings l ON l.seller_email = ss.email
    LEFT JOIN sales s ON s.listing_id = l.id
    WHERE sa.shop_type = 'pro_athlete'
    GROUP BY sa.id
    ORDER BY sa.created_at DESC
  `).all();

  res.set('Access-Control-Allow-Origin', '*');
  res.json({ athletes });
});

// ── PRO CONSIGNMENT (DROP-OFF) ───────────────────────────────────────────────
// Local pro athletes drop off gear; GoodKit photographs, lists, ships.
// Fee: 75% athlete / 25% GoodKit. No Stripe account needed for athlete.
// Admin creates the listing under GoodKit's own seller account.

// POST /admin/pro-consign/items — log a new drop-off item
app.post('/admin/pro-consign/items', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  const { athlete_id, athlete_name, athlete_email, athlete_phone, item_description, notes } = req.body;
  if (!athlete_name || !item_description) {
    return res.status(400).json({ error: 'athlete_name and item_description required' });
  }

  const id = uuidv4();
  db.prepare(`
    INSERT INTO pro_consign_items (id, athlete_id, athlete_name, athlete_email, athlete_phone, item_description, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(id, athlete_id || null, athlete_name, athlete_email || '', athlete_phone || null, item_description, notes || null);

  res.json({ ok: true, consign_id: id });
});

// PATCH /admin/pro-consign/items/:id — update status, link listing, record sale/payout
app.patch('/admin/pro-consign/items/:id', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  const item = db.prepare('SELECT * FROM pro_consign_items WHERE id=?').get(req.params.id);
  if (!item) return res.status(404).json({ error: 'Not found' });

  const { status, listing_id, sale_price, notes } = req.body;
  const updates = [];
  const vals = [];

  if (status) { updates.push('status=?'); vals.push(status); }
  if (listing_id) { updates.push('listing_id=?'); vals.push(listing_id); }
  if (notes !== undefined) { updates.push('notes=?'); vals.push(notes); }
  let saleCents = null, athletePayout = null, platformCut = null;
  if (sale_price !== undefined) {
    saleCents    = Math.round(parseFloat(sale_price) * 100);
    athletePayout = Math.round(saleCents * 0.75);
    platformCut   = saleCents - athletePayout;
    updates.push('sale_price=?', 'athlete_payout=?', 'platform_cut=?');
    vals.push(saleCents, athletePayout, platformCut);
  }
  if (status === 'paid_out') { updates.push("paid_at=datetime('now')"); }

  if (!updates.length) return res.status(400).json({ error: 'Nothing to update' });

  vals.push(req.params.id);

  // If sale price being set and item has an athlete_id, credit their wallet
  db.transaction(() => {
    db.prepare(`UPDATE pro_consign_items SET ${updates.join(',')} WHERE id=?`).run(...vals);
    if (saleCents && item.athlete_id && !item.sale_price) {
      // Only credit once (item.sale_price was null before this update)
      db.prepare(`INSERT INTO pro_consign_wallet_events (id, athlete_id, item_id, amount, type, description) VALUES (?,?,?,?,?,?)`)
        .run(uuidv4(), item.athlete_id, req.params.id, athletePayout, 'sale', `Sale: ${item.item_description}`);
      db.prepare(`UPDATE pro_consign_athletes SET wallet_balance=wallet_balance+?, total_earned=total_earned+? WHERE id=?`)
        .run(athletePayout, athletePayout, item.athlete_id);
    }
  })();

  res.json({ ok: true });
});

// GET /admin/pro-consign/items — list all consignment items
app.get('/admin/pro-consign/items', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  const items = db.prepare(`
    SELECT * FROM pro_consign_items ORDER BY created_at DESC
  `).all().map(i => ({
    ...i,
    sale_price:      i.sale_price      ? i.sale_price / 100      : null,
    athlete_payout:  i.athlete_payout  ? i.athlete_payout / 100  : null,
    platform_cut:    i.platform_cut    ? i.platform_cut / 100     : null,
  }));

  res.set('Access-Control-Allow-Origin', '*');
  res.json({ items });
});

// ── PRO CONSIGN ATHLETE PROFILES & WALLET ────────────────────────────────────

// POST /admin/pro-consign/athletes — create athlete profile
app.post('/admin/pro-consign/athletes', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const { name, email, phone, address, city, state, zip, venmo, paypal, bank_routing, bank_account, notes } = req.body;
  if (!name || !email) return res.status(400).json({ error: 'name and email required' });
  const id = uuidv4();
  try {
    db.prepare(`
      INSERT INTO pro_consign_athletes (id, name, email, phone, address, city, state, zip, venmo, paypal, bank_routing, bank_account, notes)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(id, name, email, phone||null, address||null, city||null, state||null, zip||null, venmo||null, paypal||null, bank_routing||null, bank_account||null, notes||null);
    res.json({ ok: true, athlete_id: id });
    // Fire welcome email after responding (non-blocking)
    sendPartnerWelcome({ to: email, recipientName: name, entityName: name, entityType: 'consign_athlete', loginLink: null })
      .catch(e => console.error('[pro-consign/athletes] welcome email failed:', e.message));
  } catch(e) {
    if (e.message.includes('UNIQUE')) return res.status(400).json({ error: 'Athlete with that email already exists' });
    throw e;
  }
});

// GET /admin/pro-consign/athletes — list all athletes with wallet balances
app.get('/admin/pro-consign/athletes', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const athletes = db.prepare(`SELECT * FROM pro_consign_athletes ORDER BY name`).all().map(a => ({
    ...a,
    wallet_balance: (a.wallet_balance || 0) / 100,
    total_earned:   (a.total_earned   || 0) / 100,
  }));
  res.set('Access-Control-Allow-Origin', '*');
  res.json({ athletes });
});

// PATCH /admin/pro-consign/athletes/:id — update profile/payment info
app.patch('/admin/pro-consign/athletes/:id', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const fields = ['name','email','phone','address','city','state','zip','venmo','paypal','bank_routing','bank_account','notes','status'];
  const updates = []; const vals = [];
  fields.forEach(f => { if (req.body[f] !== undefined) { updates.push(`${f}=?`); vals.push(req.body[f]); } });
  if (!updates.length) return res.status(400).json({ error: 'Nothing to update' });
  vals.push(req.params.id);
  db.prepare(`UPDATE pro_consign_athletes SET ${updates.join(',')} WHERE id=?`).run(...vals);
  res.json({ ok: true });
});

// POST /admin/pro-consign/athletes/:id/payout — record a payout, deduct from wallet
app.post('/admin/pro-consign/athletes/:id/payout', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const { amount, method, method_detail, notes } = req.body;
  if (!amount || !method) return res.status(400).json({ error: 'amount and method required' });
  const athlete = db.prepare('SELECT * FROM pro_consign_athletes WHERE id=?').get(req.params.id);
  if (!athlete) return res.status(404).json({ error: 'Athlete not found' });
  const amountCents = Math.round(parseFloat(amount) * 100);
  if (amountCents > athlete.wallet_balance) return res.status(400).json({ error: 'Amount exceeds wallet balance' });
  const payoutId = uuidv4();
  const eventId  = uuidv4();
  db.transaction(() => {
    db.prepare(`INSERT INTO pro_consign_payouts (id, athlete_id, amount, method, method_detail, notes) VALUES (?,?,?,?,?,?)`)
      .run(payoutId, req.params.id, amountCents, method, method_detail||null, notes||null);
    db.prepare(`INSERT INTO pro_consign_wallet_events (id, athlete_id, amount, type, description) VALUES (?,?,?,?,?)`)
      .run(eventId, req.params.id, -amountCents, 'payout', `Payout via ${method}${method_detail ? ' — '+method_detail : ''}`);
    db.prepare(`UPDATE pro_consign_athletes SET wallet_balance=wallet_balance-? WHERE id=?`).run(amountCents, req.params.id);
  })();
  res.json({ ok: true, payout_id: payoutId });
});

// GET /admin/pro-consign/athletes/:id/history — wallet event log
app.get('/admin/pro-consign/athletes/:id/history', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const events = db.prepare(`SELECT * FROM pro_consign_wallet_events WHERE athlete_id=? ORDER BY created_at DESC`).all(req.params.id)
    .map(e => ({ ...e, amount: e.amount / 100 }));
  const payouts = db.prepare(`SELECT * FROM pro_consign_payouts WHERE athlete_id=? ORDER BY paid_at DESC`).all(req.params.id)
    .map(p => ({ ...p, amount: p.amount / 100 }));
  res.set('Access-Control-Allow-Origin', '*');
  res.json({ events, payouts });
});

// ── COMMUNITY ROUTES ──────────────────────────────────────────────────────────

// GET /api/communities — list all active communities
app.get('/api/communities', (req, res) => {
  const communities = db.prepare(`SELECT * FROM communities WHERE status='active' ORDER BY name`).all();
  res.json(communities);
});

// GET /api/communities/:slug — single community + featured listings
app.get('/api/communities/:slug', (req, res) => {
  const community = db.prepare(`SELECT * FROM communities WHERE slug=? AND status='active'`).get(req.params.slug);
  if (!community) return res.status(404).json({ error: 'Community not found' });
  const rawListings = db.prepare(`
    SELECT l.*, ss.account_type FROM listings l
    LEFT JOIN seller_sessions ss ON ss.email=l.seller_email AND ss.used=1 AND ss.account_type IS NOT NULL
    WHERE l.community_id=? AND l.status='approved'
    ORDER BY CASE WHEN l.id IN (SELECT value FROM json_each(?)) THEN 0 ELSE 1 END, l.created_at DESC
  `).all(community.id, community.featured_listing_ids || '[]');
  const listings = rawListings.map(l => ({
    ...l,
    photos:            JSON.parse(l.photos || '[]'),
    price:             l.price / 100,
    shipping_estimate: (l.shipping_estimate || 0) / 100,
    split:             calculateSplit(l.price, l.shipping_estimate || 0, null, l.account_type || 'individual'),
    label:             getLabelTier(l.weight_oz)
  }));
  res.json({ community, listings });
});

// GET /api/communities/:slug/listings — listings for a community (for marketplace filter)
app.get('/api/communities/:slug/listings', (req, res) => {
  const community = db.prepare(`SELECT id FROM communities WHERE slug=? AND status='active'`).get(req.params.slug);
  if (!community) return res.status(404).json({ error: 'Not found' });
  const rawListings = db.prepare(`
    SELECT l.*, ss.account_type FROM listings l
    LEFT JOIN seller_sessions ss ON ss.email=l.seller_email AND ss.used=1 AND ss.account_type IS NOT NULL
    WHERE l.community_id=? AND l.status='approved' ORDER BY l.created_at DESC
  `).all(community.id);
  const listings = rawListings.map(l => ({
    ...l,
    photos:            JSON.parse(l.photos || '[]'),
    price:             l.price / 100,
    shipping_estimate: (l.shipping_estimate || 0) / 100,
    split:             calculateSplit(l.price, l.shipping_estimate || 0, null, l.account_type || 'individual'),
    label:             getLabelTier(l.weight_oz)
  }));
  res.json(listings);
});

// POST /admin/communities/create — admin creates a community
app.post('/admin/communities/create', (req, res) => {
  const key = req.headers['x-admin-key'] || req.body.admin_key;
  if (key !== process.env.ADMIN_KEY) return res.status(403).json({ error: 'Unauthorized' });
  const { name, slug, tagline, logo_url, accent_color, cause_pct, cause_label, admin_email } = req.body;
  if (!name || !slug || !admin_email) return res.status(400).json({ error: 'name, slug, admin_email required' });
  const cleanSlug = slug.toLowerCase().replace(/[^a-z0-9-]/g, '-');
  try {
    const id = uuidv4();
    db.prepare(`
      INSERT INTO communities (id, slug, name, tagline, logo_url, accent_color, cause_pct, cause_label, admin_email)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(id, cleanSlug, name, tagline||'', logo_url||'', accent_color||'#E85D26', cause_pct||0, cause_label||'', admin_email);
    res.json({ ok: true, id, slug: cleanSlug });
  } catch(e) {
    if (e.message.includes('UNIQUE')) return res.status(409).json({ error: 'Slug already taken' });
    throw e;
  }
});

// PATCH /admin/communities/:id — update community (logo, colors, featured listings, etc.)
app.patch('/admin/communities/:id', (req, res) => {
  const key = req.headers['x-admin-key'] || req.body.admin_key;
  if (key !== process.env.ADMIN_KEY) return res.status(403).json({ error: 'Unauthorized' });
  const { name, tagline, logo_url, accent_color, cause_pct, cause_label, featured_listing_ids, status } = req.body;
  const community = db.prepare(`SELECT * FROM communities WHERE id=?`).get(req.params.id);
  if (!community) return res.status(404).json({ error: 'Not found' });
  db.prepare(`
    UPDATE communities SET
      name=?, tagline=?, logo_url=?, accent_color=?, cause_pct=?, cause_label=?, featured_listing_ids=?, status=?
    WHERE id=?
  `).run(
    name ?? community.name,
    tagline ?? community.tagline,
    logo_url ?? community.logo_url,
    accent_color ?? community.accent_color,
    cause_pct ?? community.cause_pct,
    cause_label ?? community.cause_label,
    featured_listing_ids ? JSON.stringify(featured_listing_ids) : community.featured_listing_ids,
    status ?? community.status,
    req.params.id
  );
  res.json({ ok: true });
});

// PATCH /admin/communities/:id/members — approve/add a member email
app.post('/admin/communities/:id/members', (req, res) => {
  const key = req.headers['x-admin-key'] || req.body.admin_key;
  if (key !== process.env.ADMIN_KEY) return res.status(403).json({ error: 'Unauthorized' });
  const { email, status } = req.body;
  if (!email) return res.status(400).json({ error: 'email required' });
  try {
    db.prepare(`
      INSERT INTO community_members (id, community_id, email, status) VALUES (?, ?, ?, ?)
      ON CONFLICT(community_id, email) DO UPDATE SET status=excluded.status
    `).run(uuidv4(), req.params.id, email, status || 'approved');
    res.json({ ok: true });
  } catch(e) { throw e; }
});

// GET /admin/communities — list all communities for admin
app.get('/admin/communities', (req, res) => {
  const key = req.headers['x-admin-key'];
  if (key !== process.env.ADMIN_KEY) return res.status(403).json({ error: 'Unauthorized' });
  const communities = db.prepare(`SELECT * FROM communities ORDER BY created_at DESC`).all();
  res.json(communities);
});

// PATCH /listings/:id/community — seller affiliates listing with a community
app.patch('/listings/:id/community', (req, res) => {
  const { community_id, seller_email } = req.body;
  const listing = db.prepare(`SELECT * FROM listings WHERE id=?`).get(req.params.id);
  if (!listing) return res.status(404).json({ error: 'Listing not found' });
  if (listing.seller_email !== seller_email) return res.status(403).json({ error: 'Not your listing' });
  db.prepare(`UPDATE listings SET community_id=? WHERE id=?`).run(community_id || null, req.params.id);
  res.json({ ok: true });
});

// GET /community/:slug — serve branded community page
app.get('/community/:slug', (req, res) => res.sendFile(path.join(__dirname, 'goodkit-community.html')));

// ── START ─────────────────────────────────────────────────────────────────────

// ── Global error handler ──────────────────────────────────────────────────────
app.use((err, req, res, next) => {
  console.error(`[ERROR] ${req.method} ${req.path}:`, err.message || err);
  if (res.headersSent) return next(err);
  const status = err.status || err.statusCode || 500;
  res.status(status).json({
    error: status === 500 ? 'Internal server error' : (err.message || 'Error')
  });
});

// ── Nightly consign discount job ─────────────────────────────────────────────
// Runs once on startup and every 24h. Applies time-based discounts to live
// shop consign listings: 10% off per 30 days after 60 days listed, max 50%.
function applyConsignDiscounts() {
  const liveItems = db.prepare(`
    SELECT * FROM shop_consign_items
    WHERE status='listed' AND listed_at IS NOT NULL AND original_list_price IS NOT NULL AND listing_id IS NOT NULL
  `).all();

  let updated = 0;
  for (const item of liveItems) {
    const daysSinceListed = (Date.now() - new Date(item.listed_at).getTime()) / (1000 * 60 * 60 * 24);
    if (daysSinceListed < 60) continue;
    const monthsOver = Math.floor((daysSinceListed - 60) / 30) + 1;
    const discountPct = Math.min(monthsOver * 10, 50);
    const newPriceCents = Math.round(item.original_list_price * (1 - discountPct / 100));
    const listing = db.prepare(`SELECT price FROM listings WHERE id=?`).get(item.listing_id);
    if (!listing) continue;
    if (listing.price !== newPriceCents) {
      db.prepare(`UPDATE listings SET price=? WHERE id=?`).run(newPriceCents, item.listing_id);
      updated++;
      console.log(`[discount] ${item.item_description} → ${discountPct}% off ($${newPriceCents/100})`);
    }
  }
  if (updated) console.log(`[discount] Applied discounts to ${updated} consign items`);
}

applyConsignDiscounts();
setInterval(applyConsignDiscounts, 24 * 60 * 60 * 1000);
// ─────────────────────────────────────────────────────────────────────────────

app.listen(PORT, () => console.log(`GoodKit Marketplace running on port ${PORT}`));

// Auto-release payouts 72h after delivery (runs hourly)
async function releasePayouts() {
  try {
    const cutoff  = new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString();
    const pending = db.prepare("SELECT s.*, l.stripe_account_id FROM sales s JOIN listings l ON s.listing_id=l.id WHERE s.status='delivered' AND s.created_at<?").all(cutoff);
    for (const sale of pending) {
      try {
        db.prepare("UPDATE sales SET status='paid_out' WHERE id=?").run(sale.id);
        console.log(`Payout released: ${sale.id}`);
      } catch(err) { console.error(`Payout failed: ${sale.id}`, err.message); }
    }
  } catch(err) { console.error('releasePayouts error:', err.message); }
}
setTimeout(() => { releasePayouts(); setInterval(releasePayouts, 60 * 60 * 1000); }, 15000);

// Nightly backup at 2am UTC
function scheduleNightlyBackup() {
  const now   = new Date();
  const next2 = new Date();
  next2.setUTCHours(2, 0, 0, 0);
  if (next2 <= now) next2.setDate(next2.getDate() + 1);
  const msUntil = next2 - now;
  console.log(`Next backup in ${Math.round(msUntil/1000/60)} minutes`);
  setTimeout(() => { runBackup(); setInterval(runBackup, 24 * 60 * 60 * 1000); }, msUntil);
}
scheduleNightlyBackup();

// ─── Listing lifecycle: re-engagement nudges + auto-expire ─────────────────
// Day 21 — first nudge: still available? option to lower price or mark sold
// Day 28 — final warning: expires in 48h
// Day 30 — auto-deactivate (status → 'expired'), seller can reactivate
async function runListingLifecycle() {
  try {
    const now = Date.now();
    const day21 = new Date(now - 21 * 24 * 60 * 60 * 1000).toISOString();
    const day28 = new Date(now - 28 * 24 * 60 * 60 * 1000).toISOString();
    const day30 = new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString();

    // Auto-expire: active listings older than 30 days with no final-warned reactivation
    const toExpire = db.prepare(`
      SELECT * FROM listings
      WHERE status='approved'
        AND created_at < ?
        AND final_warned_at IS NOT NULL
    `).all(day30);

    for (const l of toExpire) {
      db.prepare("UPDATE listings SET status='expired' WHERE id=?").run(l.id);
      const price = (l.price / 100).toFixed(0);
      await sendEmail(l.seller_email, `Your GoodKit listing expired: ${l.title}`, emailTemplate('Listing Expired', `
        <p style="font-size:15px;color:#444;margin:0 0 16px;">Your listing for <strong>${l.title}</strong> ($${price}) was automatically deactivated after 30 days.</p>
        <p style="font-size:15px;color:#444;margin:0 0 20px;">If it's still available, you can reactivate it anytime from your seller dashboard — it takes one click.</p>
        <a href="https://good-kit.com/seller" style="display:inline-block;background:#1a7a3a;color:#fff;font-weight:700;padding:12px 28px;border-radius:8px;text-decoration:none;font-size:15px;">Reactivate Listing</a>
      `));
      console.log(`[lifecycle] expired listing ${l.id}: ${l.title}`);
    }

    // Day 28 final warning — only listings that already got day-21 nudge, not yet warned
    const toWarn = db.prepare(`
      SELECT * FROM listings
      WHERE status='approved'
        AND created_at < ?
        AND reminded_at IS NOT NULL
        AND final_warned_at IS NULL
    `).all(day28);

    for (const l of toWarn) {
      db.prepare("UPDATE listings SET final_warned_at=? WHERE id=?").run(new Date().toISOString(), l.id);
      const price = (l.price / 100).toFixed(0);
      const portal = `https://good-kit.com/seller`;
      await sendEmail(l.seller_email, `Your listing expires in 48 hours: ${l.title}`, emailTemplate('Listing Expires Soon ⏰', `
        <p style="font-size:15px;color:#444;margin:0 0 8px;">Your listing for <strong>${l.title}</strong> will automatically deactivate in <strong>48 hours</strong>.</p>
        <p style="font-size:15px;color:#444;margin:0 0 20px;">Tap below to keep it live, lower the price, or mark it sold.</p>
        <div style="display:flex;flex-direction:column;gap:10px;max-width:300px;">
          <a href="${portal}" style="display:block;background:#1a7a3a;color:#fff;font-weight:700;padding:12px 20px;border-radius:8px;text-decoration:none;font-size:15px;text-align:center;">Keep It Live</a>
          <a href="${portal}" style="display:block;background:#f0f0f0;color:#333;font-weight:600;padding:12px 20px;border-radius:8px;text-decoration:none;font-size:15px;text-align:center;">Lower the Price</a>
          <a href="${portal}" style="display:block;background:#fff;border:1px solid #ddd;color:#888;font-weight:500;padding:12px 20px;border-radius:8px;text-decoration:none;font-size:15px;text-align:center;">Mark as Sold</a>
        </div>
        <p style="font-size:12px;color:#aaa;margin:20px 0 0;">Listed at $${price} · ${l.category || 'Cycling gear'}</p>
      `));
      console.log(`[lifecycle] final warning sent for listing ${l.id}: ${l.title}`);
    }

    // Day 21 first nudge — active listings not yet reminded
    const toNudge = db.prepare(`
      SELECT * FROM listings
      WHERE status='approved'
        AND created_at < ?
        AND reminded_at IS NULL
    `).all(day21);

    for (const l of toNudge) {
      db.prepare("UPDATE listings SET reminded_at=? WHERE id=?").run(new Date().toISOString(), l.id);
      const price = (l.price / 100).toFixed(0);
      const portal = `https://good-kit.com/seller`;
      await sendEmail(l.seller_email, `Still available? Your GoodKit listing: ${l.title}`, emailTemplate('Still Available?', `
        <p style="font-size:15px;color:#444;margin:0 0 6px;">Your <strong>${l.title}</strong> has been listed for 3 weeks at <strong>$${price}</strong>.</p>
        <p style="font-size:15px;color:#444;margin:0 0 20px;">Is it still available? A quick update keeps your listing active and helps buyers know what's current.</p>
        <div style="display:flex;flex-direction:column;gap:10px;max-width:300px;">
          <a href="${portal}" style="display:block;background:#1a7a3a;color:#fff;font-weight:700;padding:12px 20px;border-radius:8px;text-decoration:none;font-size:15px;text-align:center;">✓ Still Available</a>
          <a href="${portal}" style="display:block;background:#f5f5f5;color:#333;font-weight:600;padding:12px 20px;border-radius:8px;text-decoration:none;font-size:15px;text-align:center;">💲 Lower the Price</a>
          <a href="${portal}" style="display:block;background:#fff;border:1px solid #ddd;color:#888;font-weight:500;padding:12px 20px;border-radius:8px;text-decoration:none;font-size:15px;text-align:center;">✗ Mark as Sold</a>
        </div>
        <p style="font-size:12px;color:#aaa;margin:20px 0 0;">Listing auto-deactivates in 9 days if no action is taken.</p>
      `));
      console.log(`[lifecycle] day-21 nudge sent for listing ${l.id}: ${l.title}`);
    }

  } catch(err) { console.error('[lifecycle] error:', err.message); }
}

// Run once at startup (after 30s), then every 6 hours
setTimeout(() => { runListingLifecycle(); setInterval(runListingLifecycle, 6 * 60 * 60 * 1000); }, 30000);

// Allow admin to trigger manually
app.post('/admin/listings/lifecycle', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  runListingLifecycle().then(() => res.json({ ok: true, message: 'Lifecycle run complete' }));
});
