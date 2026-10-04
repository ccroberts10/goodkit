require('dotenv').config();
const express = require('express');
const cors = require('cors');
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

  return {
    labelUrl:       transaction.label_url,
    trackingNumber: transaction.tracking_number,
    trackingUrl:    transaction.tracking_url_provider,
    carrier:        cheapest.provider,
    service:        cheapest.servicelevel?.name || cheapest.servicelevel_name,
    rate:           cheapest.amount
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
].forEach(sql => { try { db.exec(sql); } catch(e) {} });

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
// Seller keeps 85%. GoodKit takes 15% of item price only.
//   - 13% goes to GoodKit platform
//   - 2% goes to CCX team fund (if listing has a ccx_code)
// Shipping passes through at cost (flat tier price charged to buyer).
// Stripe fee (~2.9% + $0.30) comes out of platform's 13%.
function calculateSplit(itemPriceCents, shippingCents, ccxCode) {
  shippingCents = shippingCents || 0;
  const totalCents    = itemPriceCents + shippingCents;
  const stripeFee     = Math.round(totalCents * 0.029 + 30);
  const platformFee   = Math.round(itemPriceCents * 0.15);   // 15% of item only
  const ccxFund       = ccxCode ? Math.round(itemPriceCents * 0.02) : 0; // 2% to team fund
  const goodkitNet    = platformFee - ccxFund;               // 13% to GoodKit
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
    sellerPct:    85,
    platformPct:  15,
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

app.get('/', (req, res) => res.json({ status: 'GoodKit Marketplace running' }));

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
    const listings = db.prepare("SELECT * FROM listings WHERE seller_email = ? ORDER BY created_at DESC").all(email)
      .map(l => ({
        ...l,
        photos: JSON.parse(l.photos || '[]'),
        price:  l.price / 100,
        shipping_estimate: (l.shipping_estimate || 0) / 100,
        split:  calculateSplit(l.price, l.shipping_estimate || 0),
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
    const { seller_name, seller_email, stripe_account_id: client_stripe_id, title, category, size, description, condition, price, shipping_estimate, weight_oz, keywords, ccx_code } = req.body;
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
    db.prepare(`INSERT INTO listings (id,seller_name,seller_email,stripe_account_id,title,category,size,description,condition,price,shipping_estimate,weight_oz,photos,status,keywords,ccx_code) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'approved',?,?)`)
      .run(id, seller_name.trim(), seller_email.trim(), stripeId, title.trim(), category || 'Other', size || '', description || '', condition || 'Good', priceInCents, shippingCents, weightOz, JSON.stringify(photos), keywords || '', validCcxCode);
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
    let listings = db.prepare("SELECT * FROM listings WHERE status='approved' ORDER BY created_at DESC").all()
      .map(l => ({
        ...l,
        photos:            JSON.parse(l.photos || '[]'),
        price:             l.price / 100,
        shipping_estimate: (l.shipping_estimate || 0) / 100,
        split:             calculateSplit(l.price, l.shipping_estimate || 0),
        label:             getLabelTier(l.weight_oz)
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

    const shippingCents = (deliveryType === 'pickup') ? 0 : (listing.shipping_estimate || 0);
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
        deliveryType: deliveryType || 'shipping',
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
    db.prepare(`INSERT INTO sales (id,listing_id,buyer_email,buyer_name,buyer_street1,buyer_city,buyer_state,buyer_zip,payment_intent_id,amount,seller_payout,platform_payout,delivery_type,label_tier,ccx_code,ccx_fund) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(saleId, listingId, buyerEmail,
        buyerName || '', buyerStreet1 || '', buyerCity || '', buyerState || '', buyerZip || '',
        paymentIntent.id, listing.price,
        Math.round(split.sellerNet * 100),
        Math.round(split.platformFee * 100),
        deliveryType || 'shipping',
        labelInfo.tier,
        listing.ccx_code || null,
        Math.round((split.ccxFund || 0) * 100)
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
      label: labelInfo
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
    const { price, category, size, keywords } = req.body;
    if (price !== undefined && (isNaN(price) || Number(price) < 1)) return res.status(400).json({ error: 'Invalid price' });
    if (price !== undefined) db.prepare("UPDATE listings SET price=? WHERE id=?").run(Math.round(Number(price) * 100), req.params.id);
    if (category !== undefined) db.prepare("UPDATE listings SET category=? WHERE id=?").run(category, req.params.id);
    if (size !== undefined) db.prepare("UPDATE listings SET size=? WHERE id=?").run(size, req.params.id);
    if (keywords !== undefined) db.prepare("UPDATE listings SET keywords=? WHERE id=?").run(keywords, req.params.id);
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── WEBHOOK ───────────────────────────────────────────────────────────────────

app.post('/webhook', async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try { event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET); }
  catch(err) { return res.status(400).send('Webhook error: ' + err.message); }

  if (event.type === 'payment_intent.succeeded') {
    const pi        = event.data.object;
    const listingId = pi.metadata.listingId;

    db.prepare("UPDATE listings SET status='sold', sold_at=datetime('now') WHERE id=?").run(listingId);
    db.prepare("UPDATE sales SET status='delivered' WHERE payment_intent_id=?").run(pi.id);

    // CCX fund credit — if listing had a ccx_code, credit the team fund
    const saleForCcx = db.prepare("SELECT ccx_code, ccx_fund FROM sales WHERE payment_intent_id=?").get(pi.id);
    if (saleForCcx?.ccx_code && saleForCcx.ccx_fund > 0) {
      db.prepare("UPDATE ccx_teams SET fund_balance=fund_balance+?, total_earned=total_earned+? WHERE code=?")
        .run(saleForCcx.ccx_fund, saleForCcx.ccx_fund, saleForCcx.ccx_code);
    }

    const listing = db.prepare("SELECT * FROM listings WHERE id=?").get(listingId);
    const sale    = db.prepare("SELECT * FROM sales WHERE payment_intent_id=?").get(pi.id);

    if (listing && sale) {
      const isPickup = sale.delivery_type === 'pickup';
      let labelUrl = null, trackingNumber = null, trackingUrl = null;

      if (!isPickup && SHIPPO_API_KEY) {
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
            db.prepare("UPDATE sales SET label_url=?, tracking_number=?, tracking_url=? WHERE id=?").run(labelUrl, trackingNumber, trackingUrl, sale.id);
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
           <p style="font-size:12px;color:#888;margin:8px 0 0;">Payout transferred automatically 72 hours after delivery confirmation.</p>
         </div>`
      ));

      // Email buyer with tracking if we got it
      if (!isPickup && trackingNumber) {
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

    // Send welcome email with team code
    await sendEmail(captain_email, `Your GoodKit CCX team code: ${code}`, emailTemplate('Welcome to GoodKit CCX 🚴',
      `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Hi ${captain_name}, your team is registered!</p>
       <div style="background:#E8E0D0;padding:20px 24px;margin-bottom:20px;">
         <p style="font-size:11px;letter-spacing:0.15em;text-transform:uppercase;color:#888;margin:0 0 4px;">${school} · ${team_name}</p>
         <p style="font-size:13px;color:#888;margin:0 0 8px;">Your CCX team code</p>
         <p style="font-size:40px;font-weight:900;color:#FF5C1A;letter-spacing:0.1em;margin:0;">${code}</p>
       </div>
       <p style="font-size:13px;color:#555;line-height:1.7;margin:0 0 16px;">Share this code with your teammates. When they list gear on GoodKit and enter <strong>${code}</strong>, 2% of every sale automatically flows into your team fund.</p>
       <p style="font-size:13px;color:#555;margin:0 0 20px;"><strong>How it works:</strong> Seller keeps 85% · 2% to team fund · 13% GoodKit fee</p>
       <a href="${BASE_URL}/ccx/dashboard?team=${id}" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Team Dashboard →</a>`
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

// GET /admin/overview — full dashboard data in one shot
app.get('/admin/overview', (req, res) => {
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
    const cutoff  = new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString();
    const pending = db.prepare("SELECT s.*, l.stripe_account_id FROM sales s JOIN listings l ON s.listing_id=l.id WHERE s.status='delivered' AND s.created_at<?").all(cutoff);
    let released  = 0;
    for (const sale of pending) {
      try {
        db.prepare("UPDATE sales SET status='paid_out' WHERE id=?").run(sale.id);
        released++;
      } catch(err) { console.error('Payout error', sale.id, err.message); }
    }
    res.json({ success: true, released });
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

// Individual listing page — crawlable, OG-tagged, links to marketplace
app.get('/listing/:id', (req, res) => {
  try {
    const listing = db.prepare("SELECT * FROM listings WHERE id=? AND status='approved'").get(req.params.id);
    if (!listing) return res.status(404).send('<h1>Listing not found</h1>');
    const photos = JSON.parse(listing.photos || '[]');
    const price  = (listing.price / 100).toFixed(2);
    const photo  = photos[0] ? `${SITE_URL}${photos[0]}` : `${SITE_URL}/icon-512.png`;
    const title  = `${listing.title} — GoodKit`;
    const desc   = listing.description
      ? listing.description.slice(0, 160)
      : `Used ${listing.category} for $${price}. ${listing.condition} condition. Buy on GoodKit, the cycling gear marketplace.`;
    const keywords = [listing.title, listing.category, listing.condition, ...(listing.keywords || '').split(',').map(k => k.trim()).filter(Boolean)].join(', ');

    res.setHeader('Content-Type', 'text/html');
    res.send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
  <title>${title}</title>
  <meta name="description" content="${desc}">
  <meta name="keywords" content="${keywords}">
  <link rel="canonical" href="${SITE_URL}/listing/${listing.id}">
  <meta property="og:type" content="product">
  <meta property="og:title" content="${listing.title}">
  <meta property="og:description" content="${desc}">
  <meta property="og:image" content="${photo}">
  <meta property="og:image:width" content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:url" content="${SITE_URL}/listing/${listing.id}">
  <meta property="og:site_name" content="GoodKit">
  <meta property="product:price:amount" content="${price}">
  <meta property="product:price:currency" content="USD">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${listing.title}">
  <meta name="twitter:description" content="${desc}">
  <meta name="twitter:image" content="${photo}">
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
      "url": `${SITE_URL}/listing/${listing.id}`,
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
  <a class="nav-cta" href="${SITE_URL}/goodkit-marketplace.html">Shop All Gear</a>
</nav>
<div class="container">
  <div class="layout">
    <div class="gallery">
      ${photos[0] ? `<img class="gallery-main" id="mainPhoto" src="${SITE_URL}${photos[0]}" alt="${listing.title}">` : `<div class="gallery-main" style="display:flex;align-items:center;justify-content:center;font-size:48px;">🚴</div>`}
      ${photos.length > 1 ? `<div class="gallery-thumbs">${photos.map((p, i) => `<img src="${SITE_URL}${p}" class="${i===0?'active':''}" onclick="setPhoto('${SITE_URL}${p}',this)" alt="Photo ${i+1}">`).join('')}</div>` : ''}
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
        <a class="cta-btn" href="${SITE_URL}/goodkit-marketplace.html#${listing.id}">Buy on GoodKit →</a>
        <div class="cta-sub">Secure checkout · Shippo-powered shipping · Seller keeps 88%</div>
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

// Sitemap — includes all active listings
app.get('/sitemap.xml', (req, res) => {
  try {
    const listings = db.prepare("SELECT id, created_at FROM listings WHERE status='approved' ORDER BY created_at DESC LIMIT 1000").all();
    const urls = [
      { loc: SITE_URL, priority: '1.0', changefreq: 'daily' },
      { loc: `${SITE_URL}/goodkit-marketplace.html`, priority: '0.9', changefreq: 'hourly' },
      ...listings.map(l => ({
        loc: `${SITE_URL}/listing/${l.id}`,
        lastmod: l.created_at.split('T')[0],
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

// ── START ─────────────────────────────────────────────────────────────────────

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
