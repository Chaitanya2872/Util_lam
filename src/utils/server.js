import express from 'express';
import dotenv from 'dotenv';
dotenv.config();
import axios from 'axios';
import cors from 'cors';
import { google } from 'googleapis';
import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import path from 'path';


const PHONE_NUMBER_ID = process.env.WHATSAPP_PHONE_NUMBER_ID;
const ACCESS_TOKEN = process.env.WHATSAPP_ACCESS_TOKEN;

async function sendWhatsAppTemplate(toNumber, leadName, assignedTo, location, remarks, leadPhone) {
  const phoneNumber = sanitizePhoneNumber(toNumber);
  if (!phoneNumber) {
    console.warn(`⚠️ Invalid phone number for WhatsApp: ${toNumber}`);
    return false;
  }

  const url = `https://graph.facebook.com/v19.0/${process.env.WHATSAPP_PHONE_NUMBER_ID}/messages`;
  const headers = {
    'Authorization': `Bearer ${process.env.WHATSAPP_ACCESS_TOKEN}`,
    'Content-Type': 'application/json'
  };

  const payload = {
    messaging_product: "whatsapp",
    to: phoneNumber,
    type: "template",
    template: {
      name: "lead_details",  // ✅ Your approved template name
      language: { code: "en_US" },
      components: [
        {
          type: "body",
          parameters: [
            { type: "text", text: assignedTo || 'N/A' },   // {{1}} Salesperson name
            { type: "text", text: leadName || 'N/A' },      // {{2}} Lead name in quotes
            { type: "text", text: leadName || 'N/A' },      // {{3}} Lead name again
            { type: "text", text: leadPhone || 'N/A' },     // {{4}} Lead phone number
            { type: "text", text: location || 'N/A' },      // {{5}} Project location
            { type: "text", text: remarks || 'N/A' }        // {{6}} Remarks
          ]
        }
      ]
    }
  };

  try {
    const response = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload)
    });

    const result = await response.json();

    if (response.ok) {
      console.log(`✅ WhatsApp message sent to ${phoneNumber}`);
      return true;
    } else {
      console.error(`❌ WhatsApp API error:`, result);
      return false;
    }
  } catch (err) {
    console.error(`❌ Exception sending WhatsApp:`, err.message);
    return false;
  }
}



async function sendWhatsAppOTP(phoneNumber, otp) {
  try {
    console.log(`✅ Sending OTP to WhatsApp for number: ${phoneNumber}`);

    const response = await axios({
      method: "POST",
      url: `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
      headers: {
        Authorization: `Bearer ${ACCESS_TOKEN}`,
        "Content-Type": "application/json",
      },
      data: {
        messaging_product: "whatsapp",
        recipient_type: "individual",
        to: phoneNumber,
        type: "template",
        template: {
  name: "send_otpmessage",
  language: { code: "en_US" },
  components: [
    {
      type: "body",
      parameters: [{ type: "text", text: otp }]
    },
    {
      type: "button",
      sub_type: "url",
      index: "0",
      parameters: [
        { type: "text", text: `https://yourapp.com` }
      ]
    }
  ]
}
      },
      timeout: 15000,
    });

    console.log("✅ OTP sent successfully");
    return { success: true, data: response.data };

  } catch (error) {
    console.error("❌ Error sending WhatsApp OTP:", error.message);

    if (error.response) {
      console.error("Status:", error.response.status);
      console.error("Error Data:", JSON.stringify(error.response.data, null, 2));
    } else {
      console.error("No response from WhatsApp API");
    }

    return { success: false, error: error.message || "Failed to send OTP" };
  }
}









// In-memory OTP Store
const otpStore = new Map();

function generateOTP() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}




function sanitizePhoneNumber(number) {
  const digits = number.replace(/\D/g, '');
  if (digits.length === 10) return `+91${digits}`;
  if (digits.startsWith('91') && digits.length === 12) return `+${digits}`;
  if (digits.startsWith('+91') && digits.length === 13) return number;
  return null;
}





const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Google Sheets Configuration
const CONFIG = {
  USERS_SHEET_ID: '1MSrLNWanOM4OcGJcT5IwD99L1-kPi6gCHVnyhIlIyP8',
  SALES_TRACKER_SHEET_ID: '1MSrLNWanOM4OcGJcT5IwD99L1-kPi6gCHVnyhIlIyP8',
  PRE_SALES_SHEET_ID: '1TEcMZJSa0lEVYPfABRWNTr3bh0KAFyC1dvLH0gxqXr8',
  SALT: 'abc123@#SaltToken!'
};

// Google Sheets Client Setup
let auth;
let sheets;

try {
  // Try to load credentials from file first
  let credentials;
  
  if (process.env.GOOGLE_CREDENTIALS) {
    // If credentials are in environment variable (for production)
    credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  } else {
    // Try to load from credentials.json file (for development)
    try {
      const fs = await import('fs');
      const credentialsFile = fs.readFileSync('./credentials.json', 'utf8');
      credentials = JSON.parse(credentialsFile);
    } catch (fileError) {
      console.error('Could not load credentials.json file:', fileError.message);
      throw new Error('Google Sheets credentials not found. Please set up credentials.json or GOOGLE_CREDENTIALS environment variable.');
    }
  }

  // Validate credentials structure
  if (!credentials.client_email || !credentials.private_key) {
    throw new Error('Invalid Google Sheets credentials. Missing client_email or private_key.');
  }

  auth = new google.auth.GoogleAuth({
    credentials: credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });

  sheets = google.sheets({ version: 'v4', auth });
  console.log('Google Sheets authentication configured successfully');

} catch (error) {
  console.error('Google Sheets setup error:', error.message);
  console.log('Running in mock mode - Google Sheets functionality will be simulated');
  


  // Set up mock mode
  auth = null;
  sheets = null;
}

// Mock data for development/testing when Google Sheets is not available

app.post('/api/send-whatsapp', authenticateToken, async (req, res) => {
    try {
        const { salespersonName, salespersonMobile, leadName, leadPhone, location, remarks } = req.body;

        if (!salespersonMobile || !leadName) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        const sent = await sendWhatsAppTemplate(
            salespersonMobile,
            leadName,
            salespersonName,
            location,
            remarks,
            leadPhone
        );

        if (sent) {
            res.json({ message: 'WhatsApp message sent successfully' });
        } else {
            res.status(500).json({ error: 'Failed to send WhatsApp message' });
        }
    } catch (err) {
        console.error('Error sending WhatsApp message:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Utility Functions
function parseSheetData(values) {
  if (!values || values.length < 2) return [];
  const headers = values[0];
  return values.slice(1).map(row => {
    const obj = {};
    headers.forEach((header, index) => {
      obj[header] = row[index] || '';
    });
    return obj;
  });
}

function generateLeadId() {
  return 'L' + Date.now().toString().slice(-6);
}

function generateUserId() {
  return 'U' + Date.now().toString().slice(-6);
}

// Authentication Middleware
function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }

  jwt.verify(token, process.env.JWT_SECRET || 'fallback_secret', (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid token' });
    }
    req.user = user;
    next();
  });
}

// Routes

async function createUserSheetIfNotExists(userFullName, userRole) {
  try {
    // Only create sheets for salespersons
    if (userRole !== 'salesperson') {
      return;
    }

    const spreadsheetId = CONFIG.SALES_TRACKER_SHEET_ID;
    const response = await sheets.spreadsheets.get({ spreadsheetId });
    const existingSheets = response.data.sheets.map(sheet => sheet.properties.title);

    if (!existingSheets.includes(userFullName)) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        resource: {
          requests: [
            {
              addSheet: {
                properties: {
                  title: userFullName,
                  gridProperties: { rowCount: 1000, columnCount: 26 }
                }
              }
            }
          ]
        }
      });

      const qualifiedHeaders = [
        'Month', 'Date', 'Source', 'Lead ID', 'Full Name', 'Phone Number', 'Email',
        'Expected Timeline', 'Property Type', 'Project Location', 'Project State',
        'Pre-Sales', 'Response', 'Pre-Sales Remarks', 'Assigned To', 'Remarks', 'Notified'
      ];

      await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: `${userFullName}!A1:Q1`,
        valueInputOption: 'RAW',
        resource: {
          values: [qualifiedHeaders]
        }
      });

      console.log(`Sheet created for salesperson: ${userFullName}`);
    }
  } catch (error) {
    console.error(`Failed to create sheet for user ${userFullName}:`, error.message);
  }
}

// 1️⃣ API: Send OTP on Registration
app.post('/api/register', async (req, res) => {
  try {
    const { fullName, email, mobile, password, role } = req.body;

    if (!fullName || !email || !mobile || !password || !role) {
      return res.status(400).json({ error: 'All fields are required' });
    }

    const otp = generateOTP();
    otpStore.set(mobile, otp);  // ✅ Store OTP temporarily (should expire after X minutes in real app)

    const otpResult = await sendWhatsAppOTP(mobile, otp);
    if (!otpResult.success) {
      return res.status(500).json({ error: 'Failed to send OTP' });
    }

    res.status(200).json({ message: 'OTP sent successfully' });

  } catch (err) {
    console.error('❌ Registration error:', err);
    res.status(500).json({ error: 'Failed to send OTP' });
  }
});


// 2️⃣ API: Verify OTP and Create User or Login if Already Exists
app.post('/api/verify-otp', async (req, res) => {
  try {
    const { fullName, email, mobile, password, role, otp } = req.body;
    if (!otp || otpStore.get(mobile) !== otp) {
      return res.status(400).json({ error: 'Invalid OTP' });
    }

    otpStore.delete(mobile);

    const userResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.USERS_SHEET_ID,
      range: 'Users!A:H'
    });

    const users = parseSheetData(userResponse.data.values);
    let user = users.find(u => u.Email.toLowerCase() === email.toLowerCase());

    if (!user) {
      const passwordHash = await bcrypt.hash(password, 10);
      const userId = generateUserId();
      await sheets.spreadsheets.values.append({
        spreadsheetId: CONFIG.USERS_SHEET_ID,
        range: 'Users!A:H',
        valueInputOption: 'RAW',
        resource: { values: [[userId, fullName, email, passwordHash, role, new Date().toISOString().split('T')[0], 'Active']] }
      });
      await createUserSheetIfNotExists(fullName, role);
      user = { ID: userId, FullName: fullName, Email: email, Role: role };
    }

    const token = jwt.sign(
      { id: user.ID, name: user.FullName, email: user.Email, role: user.Role },
      process.env.JWT_SECRET || 'fallback_secret',
      { expiresIn: '24h' }
    );

    res.json({
      token,
      user: { id: user.ID, name: user.FullName, email: user.Email, role: user.Role },
      message: 'Verified and logged in'
    });

  } catch (err) {
    console.error('OTP verification error:', err);
    res.status(500).json({ error: 'Verification failed' });
  }
});


// 1. Modified Authentication Route with Auto User Creation
app.post('/api/login', async (req, res) => {
    try {
        const { username, password, role } = req.body;

        if (!username || !password || !role) {
            return res.status(400).json({ error: 'Username, password, and role are required' });
        }

        const userResponse = await sheets.spreadsheets.values.get({
            spreadsheetId: CONFIG.USERS_SHEET_ID,
            range: 'Users!A:H'
        });
        const users = parseSheetData(userResponse.data.values);

        let user = users.find(u =>
            u.Email.toLowerCase() === username.toLowerCase() &&
            u.Role.toLowerCase() === role.toLowerCase()
        );

        if (user) {
            const isValidPassword = await bcrypt.compare(password, user['Password Hash']);
            if (!isValidPassword) {
                return res.status(401).json({ error: 'Invalid credentials' });
            }
        } else {
            const passwordHash = await bcrypt.hash(password, 10);
            const newUserId = generateUserId();

            const newUserData = [
                newUserId,
                '',                 // ✅ FullName left empty
                username,           // ✅ Email used as username
                '',                 // ✅ Mobile Number blank
                passwordHash,       // ✅ Password Hash
                role,
                new Date().toISOString().split('T')[0],
                'Active'
            ];

            await sheets.spreadsheets.values.append({
                spreadsheetId: CONFIG.USERS_SHEET_ID,
                range: 'Users!A:H',
                valueInputOption: 'RAW',
                insertDataOption: 'INSERT_ROWS',
                resource: { values: [newUserData] }
            });

            user = {
                ID: newUserId,
                FullName: '',
                Email: username,
                Role: role
            };
        }

        const token = jwt.sign({
            id: user.ID,
            name: user.FullName,
            email: user.Email,
            role: user.Role
        }, process.env.JWT_SECRET || 'fallback_secret', { expiresIn: '24h' });

        res.json({
            token,
            user: {
                id: user.ID,
                name: user.FullName,
                email: user.Email,
                role: user.Role
            },
            message: users.find(u => u.Email.toLowerCase() === username.toLowerCase())
                ? 'Login successful'
                : 'Account created and logged in'
        });

    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Server error during login' });
    }
});




// 2. Get Qualified Leads


// Replace API to Assign Leads
// Fixed version of the assign-leads endpoint
app.post('/api/assign-leads', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'manager') {
            return res.status(403).json({ error: 'Access denied' });
        }

        const { leadIds, salesperson, salespersonId } = req.body;

        if (!leadIds || !salesperson || !salespersonId) {
            return res.status(400).json({ error: 'Lead IDs, salesperson name, and salespersonId are required' });
        }

        // Fetch all users
        const userResponse = await sheets.spreadsheets.values.get({
            spreadsheetId: CONFIG.USERS_SHEET_ID,
            range: 'Users!A:H'
        });
        const users = parseSheetData(userResponse.data.values);
        const salespersonUser = users.find(u => u.FullName === salesperson || u.ID === salespersonId);

        if (!salespersonUser) {
            return res.status(400).json({ error: 'Salesperson not found' });
        }

        // Fetch all leads
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
            range: 'Qualified!A:Z'
        });

        const values = response.data.values;
        const headers = values[0];

        const assignedToIndex = headers.indexOf('Assigned To');
        const assignedToIdIndex = headers.indexOf('Assigned To ID') !== -1 ? 
            headers.indexOf('Assigned To ID') : headers.indexOf('Assigned To Id'); // Handle both variations
        const leadIdIndex = headers.indexOf('Lead ID');

        if (assignedToIndex === -1 || assignedToIdIndex === -1 || leadIdIndex === -1) {
            console.log('Headers:', headers);
            console.log('assignedToIndex:', assignedToIndex, 'assignedToIdIndex:', assignedToIdIndex, 'leadIdIndex:', leadIdIndex);
            return res.status(500).json({ error: 'Required columns missing in Qualified sheet' });
        }

        const updates = [];
        const leadsToAssign = [];

        for (let i = 1; i < values.length; i++) {
            const row = values[i];
            const leadId = row[leadIdIndex];

            if (leadIds.includes(leadId)) {
                // ✅ Store salesperson NAME in "Assigned To" and USER ID in "Assigned To ID"
                row[assignedToIndex] = salespersonUser.FullName;
                row[assignedToIdIndex] = salespersonUser.ID;

                updates.push({
                    range: `Qualified!A${i + 1}:Z${i + 1}`,
                    values: [row]
                });

                const leadData = {};
                headers.forEach((header, idx) => {
                    leadData[header] = row[idx] || '';
                });

                leadsToAssign.push(leadData);
            }
        }

        if (updates.length > 0) {
            await sheets.spreadsheets.values.batchUpdate({
                spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
                resource: {
                    data: updates,
                    valueInputOption: 'RAW'
                }
            });
        }

        // Add leads to salesperson's individual sheet
        const spreadsheetResponse = await sheets.spreadsheets.get({
            spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID
        });
        const existingSheets = spreadsheetResponse.data.sheets.map(sheet => sheet.properties.title);

        if (!existingSheets.includes(salespersonUser.FullName)) {
            await sheets.spreadsheets.batchUpdate({
                spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
                resource: {
                    requests: [{
                        addSheet: {
                            properties: {
                                title: salespersonUser.FullName,
                                gridProperties: { rowCount: 1000, columnCount: 26 }
                            }
                        }
                    }]
                }
            });

            const qualifiedHeaders = [
                'Month', 'Date', 'Source', 'Lead ID', 'Full Name', 'Phone Number', 'Email',
                'Expected Timeline', 'Property Type', 'Project Location', 'Project State',
                'Pre-Sales', 'Response', 'Pre-Sales Remarks', 'Assigned To', 'Assigned To ID',
                'Remarks', 'Notified'
            ];

            await sheets.spreadsheets.values.update({
                spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
                range: `${salespersonUser.FullName}!A1:R1`,
                valueInputOption: 'RAW',
                resource: { values: [qualifiedHeaders] }
            });
        }

        const existingLeadsResponse = await sheets.spreadsheets.values.get({
            spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
            range: `${salespersonUser.FullName}!A:Z`
        });

        const existingLeads = parseSheetData(existingLeadsResponse.data.values);
        const existingLeadIds = existingLeads.map(lead => lead['Lead ID']);

        const newLeadsToAdd = leadsToAssign.filter(lead => !existingLeadIds.includes(lead['Lead ID']));

        // Send WhatsApp Message to Salesperson after assigning
        if (newLeadsToAdd.length > 0) {
            const salespersonMobile = salespersonUser['Mobile Number'] || salespersonUser['Mobile'];
            
            if (salespersonMobile) {
                for (const lead of newLeadsToAdd) {
                    const messageSent = await sendWhatsAppTemplate(
                        salespersonMobile,
                        lead['Full Name'],
                        salespersonUser.FullName,
                        lead['Project Location'],
                        lead['Remarks'] || '',
                        lead['Phone Number']
                    );

                    if (!messageSent) {
                        console.warn(`⚠️ WhatsApp message failed for lead ${lead['Lead ID']}`);
                    }
                }
            } else {
                console.warn(`⚠️ No mobile number found for salesperson ${salespersonUser.FullName}`);
            }
        }

        if (newLeadsToAdd.length > 0) {
            const currentDate = new Date();
            const month = currentDate.toLocaleString('default', { month: 'long' });

            const salespersonLeadData = newLeadsToAdd.map(lead => [
                month,
                lead['Date'] || currentDate.toISOString().split('T')[0],
                lead['Source'] || '',
                lead['Lead ID'] || '',
                lead['Full Name'] || '',
                lead['Phone Number'] || '',
                lead['Email'] || '',
                lead['Expected Timeline'] || '',
                lead['Property Type'] || '',
                lead['Project Location'] || '',
                '', // Project State
                'qualified', // Pre-Sales
                '', // Response
                lead['Pre-Sales Remarks'] || '',
                salespersonUser.FullName, // ✅ Store salesperson NAME
                salespersonUser.ID,       // ✅ Store salesperson ID
                '', // Remarks
                'No' // Notified
            ]);

            await sheets.spreadsheets.values.append({
                spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
                range: `${salespersonUser.FullName}!A:R`,
                valueInputOption: 'RAW',
                insertDataOption: 'INSERT_ROWS',
                resource: { values: salespersonLeadData }
            });
        }

        return res.json({
            message: `${leadIds.length} leads assigned to ${salespersonUser.FullName} successfully`,
            assignedCount: leadIds.length,
            addedToUserSheet: newLeadsToAdd.length,
            skippedDuplicates: leadsToAssign.length - newLeadsToAdd.length,
            salesperson: salespersonUser.FullName,
            salespersonId: salespersonUser.ID
        });

    } catch (err) {
        console.error('Assign leads error:', err);
        res.status(500).json({ error: 'Failed to assign leads' });
    }
});

// Fixed version of the get lead by ID endpoint
app.get('/api/leads/:leadId', authenticateToken, async (req, res) => {
    const { leadId } = req.params;

    try {
        // First, get the lead from the Qualified sheet to find who it's assigned to
        const qualifiedResponse = await sheets.spreadsheets.values.get({
            spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
            range: 'Qualified!A:Z'
        });

        const qualifiedLeads = parseSheetData(qualifiedResponse.data.values);
        const targetLead = qualifiedLeads.find(lead => lead['Lead ID'] === leadId);

        if (!targetLead) {
            return res.status(404).json({ error: 'Lead not found in Qualified sheet' });
        }

        // Get the assigned salesperson info
        const assignedTo = targetLead['Assigned To'];
        const assignedToId = targetLead['Assigned To ID'] || targetLead['Assigned To Id']; // Handle both variations

        console.log('Lead found in Qualified sheet:', {
            leadId,
            assignedTo,
            assignedToId
        });

        // If lead is assigned, try to find it in the salesperson's individual sheet
        if (assignedTo && assignedToId) {
            // Get user info to find the correct sheet name
            const userResponse = await sheets.spreadsheets.values.get({
                spreadsheetId: CONFIG.USERS_SHEET_ID,
                range: 'Users!A:H'
            });
            const users = parseSheetData(userResponse.data.values);
            
            // Find user by ID or name
            const salesperson = users.find(u => 
                u.ID === assignedToId || 
                u.FullName === assignedTo ||
                u.ID === assignedTo  // In case assignedTo contains user ID
            );

            if (salesperson) {
                try {
                    const sheetResponse = await sheets.spreadsheets.values.get({
                        spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
                        range: `${salesperson.FullName}!A:Z`
                    });

                    const leads = parseSheetData(sheetResponse.data.values);
                    const leadInSheet = leads.find(lead => lead['Lead ID'] === leadId);

                    if (leadInSheet) {
                        return res.json({ 
                            ...leadInSheet, 
                            sheet: salesperson.FullName,
                            salespersonId: salesperson.ID,
                            salespersonName: salesperson.FullName
                        });
                    }
                } catch (sheetError) {
                    console.warn(`Could not access sheet for ${salesperson.FullName}: ${sheetError.message}`);
                }
            }
        }

        // If not found in individual sheet, return data from Qualified sheet
        return res.json({ 
            ...targetLead, 
            sheet: 'Qualified',
            salespersonId: assignedToId,
            salespersonName: assignedTo
        });

    } catch (err) {
        console.error('Search lead error:', err);
        res.status(500).json({ error: 'Server error searching for lead' });
    }
});

// Updated get qualified leads endpoint to handle the column name variations
app.get('/api/qualified-leads', authenticateToken, async (req, res) => {
    try {
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
            range: 'Qualified!A:Z'
        });
        const leads = parseSheetData(response.data.values);

        // Role-based filtering
        let visibleLeads = leads;
        if (req.user.role === 'salesperson') {
            visibleLeads = leads.filter(lead => {
                const assignedToId = lead['Assigned To ID'] || lead['Assigned To Id']; // Handle both variations
                const assignedTo = lead['Assigned To'];
                
                // Check both ID and name for compatibility
                return assignedToId === req.user.id || assignedTo === req.user.name;
            });
        }

        // Transform leads to match frontend expectations
        const transformedLeads = visibleLeads.map(lead => ({
            id: lead['Lead ID'],
            name: lead['Full Name'],
            phone: lead['Phone Number'],
            email: lead['Email'],
            source: lead['Source'],
            timeline: lead['Expected Timeline'],
            propertyType: lead['Property Type'],
            location: lead['Project Location'],
            assignedTo: lead['Assigned To'],
            assignedToId: lead['Assigned To ID'] || lead['Assigned To Id'], // Handle both variations
            remarks: lead['Remarks'],
            status: lead['Assigned To'] ? 'assigned' : 'qualified',
            date: lead['Date']
        }));

        res.json(transformedLeads);
    } catch (error) {
        console.error('Get qualified leads error:', error);
        res.status(500).json({ error: 'Failed to fetch qualified leads' });
    }
});




// 4. Update Lead Remarks
// 4. Update Lead Remarks
app.put('/api/leads/:leadId/remarks', authenticateToken, async (req, res) => {
  try {
    const { leadId } = req.params;
    const { remarks, status, nextFollowUp } = req.body;

    // Get current leads from Qualified sheet
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
      range: 'Qualified!A:Z'
    });

    const values = response.data.values;
    const headers = values[0];
    const leadIdIndex = headers.indexOf('Lead ID');
    const remarksIndex = headers.indexOf('Remarks');
    const assignedToIndex = headers.indexOf('Assigned To');

    let updatedLead = null;

    // Update in Qualified sheet
    for (let i = 1; i < values.length; i++) {
      const row = values[i];
      if (row[leadIdIndex] === leadId) {
        // Salesperson can only update their own leads
        if (req.user.role === 'salesperson' && row[assignedToIndex] !== req.user.name) {
          return res.status(403).json({ error: 'Access denied' });
        }

        row[remarksIndex] = remarks || row[remarksIndex];

        if (status) {
          const statusIndex = headers.indexOf('Status');
          if (statusIndex !== -1) row[statusIndex] = status;
        }

        if (nextFollowUp) {
          const followUpIndex = headers.indexOf('Next Follow-up');
          if (followUpIndex !== -1) row[followUpIndex] = nextFollowUp;
        }

        await sheets.spreadsheets.values.update({
          spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
          range: `Qualified!A${i + 1}:Z${i + 1}`,
          valueInputOption: 'RAW',
          resource: { values: [row] }
        });

        updatedLead = row;
        break;
      }
    }

    if (!updatedLead) {
      return res.status(404).json({ error: 'Lead not found' });
    }

    // 🔁 Also update remarks in the salesperson’s sheet
    const userSheetResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.USERS_SHEET_ID,
      range: 'Users!A:H'
    });
    const users = parseSheetData(userSheetResponse.data.values);
    const salespersonName = updatedLead[assignedToIndex];
    const salesperson = users.find(u => u.FullName === salespersonName);

    if (salesperson) {
      try {
        const sheetName = salesperson.FullName;
        const indivSheet = await sheets.spreadsheets.values.get({
          spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
          range: `${sheetName}!A:Z`
        });

        const sheetRows = indivSheet.data.values;
        const sheetHeaders = sheetRows[0];
        const sheetLeadIdIndex = sheetHeaders.indexOf('Lead ID');
        const sheetRemarksIndex = sheetHeaders.indexOf('Remarks');
        const sheetStatusIndex = sheetHeaders.indexOf('Status');
        const sheetFollowUpIndex = sheetHeaders.indexOf('Next Follow-up');

        for (let j = 1; j < sheetRows.length; j++) {
          const row = sheetRows[j];
          if (row[sheetLeadIdIndex] === leadId) {
            row[sheetRemarksIndex] = remarks || row[sheetRemarksIndex];

            if (status && sheetStatusIndex !== -1) {
              row[sheetStatusIndex] = status;
            }

            if (nextFollowUp && sheetFollowUpIndex !== -1) {
              row[sheetFollowUpIndex] = nextFollowUp;
            }

            await sheets.spreadsheets.values.update({
              spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
              range: `${sheetName}!A${j + 1}:Z${j + 1}`,
              valueInputOption: 'RAW',
              resource: { values: [row] }
            });

            break;
          }
        }
      } catch (err) {
        console.warn(`⚠️ Could not update lead in ${salespersonName}'s sheet:`, err.message);
      }
    }

    return res.json({ message: 'Remarks updated successfully' });

  } catch (error) {
    console.error('Update remarks error:', error);
    res.status(500).json({ error: 'Failed to update remarks' });
  }
});


// 5. Get Dashboard Stats
app.get('/api/dashboard-stats', authenticateToken, async (req, res) => {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
      range: 'Qualified!A:Z'
    });
    const leads = parseSheetData(response.data.values);

    // Filter based on role
    let filteredLeads = leads;
    if (req.user.role === 'salesperson') {
      filteredLeads = leads.filter(lead => 
        lead['Assigned To'] === req.user.name
      );
    }

    // Calculate stats
    const today = new Date().toISOString().split('T')[0];
    const stats = {
      totalLeads: filteredLeads.length,
      assignedLeads: filteredLeads.filter(lead => lead['Assigned To']).length,
      pendingLeads: filteredLeads.filter(lead => !lead['Assigned To']).length,
      contactedToday: filteredLeads.filter(lead => {
        const hasRemarks = lead['Remarks'] && lead['Remarks'].trim();
        const isToday = lead['Date'] === today;
        return hasRemarks && isToday;
      }).length
    };

    res.json(stats);
  } catch (error) {
    console.error('Dashboard stats error:', error);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

// 6. Import Qualified Leads (Manager only)
app.post('/api/import-leads', authenticateToken, async (req, res) => {
  try {
    if (req.user.role !== 'manager') {
      return res.status(403).json({ error: 'Access denied' });
    }

    // This would implement the logic to import leads from various sources
    // For now, we'll simulate the process
    
    res.json({ message: 'Leads imported successfully' });
  } catch (error) {
    console.error('Import leads error:', error);
    res.status(500).json({ error: 'Failed to import leads' });
  }
});

app.post('/api/import-qualified-leads', authenticateToken, async (req, res) => {
  try {
    if (req.user.role !== 'manager') {
      return res.status(403).json({ error: 'Access denied' });
    }

    // Get data from Pre-Sales sheet
    const preSalesResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.PRE_SALES_SHEET_ID,
      range: 'Pre-Sales!A:Z'
    });
    const preSalesData = parseSheetData(preSalesResponse.data.values);

    // Filter only qualified leads
    const qualifiedLeads = preSalesData.filter(lead => 
      lead['Pre-Sales'] && lead['Pre-Sales'].toLowerCase() === 'qualified'
    );

    if (qualifiedLeads.length === 0) {
      return res.json({ message: 'No qualified leads found to import' });
    }

    // Get existing leads from Qualified sheet
    const existingResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
      range: 'Qualified!A:Z'
    });
    const existingLeads = parseSheetData(existingResponse.data.values);
    const existingLeadIds = existingLeads.map(lead => lead['Lead ID']);

    // Filter out already imported leads
    const newLeads = qualifiedLeads.filter(lead => 
      !existingLeadIds.includes(lead['Lead ID'])
    );

    if (newLeads.length === 0) {
      return res.json({ message: 'All qualified leads are already imported' });
    }

    // Prepare data for import
    const headers = [
      'Lead ID', 'Full Name', 'Phone Number', 'Email', 'Source', 
      'Expected Timeline', 'Property Type', 'Project Location', 
      'Assigned To', 'Remarks', 'Date'
    ];

    const importData = newLeads.map(lead => [
      lead['Lead ID'],
      lead['Full Name'],
      lead['Phone Number'],
      lead['Email'],
      lead['Source'],
      lead['Expected Timeline'],
      lead['Property Type'],
      lead['Project Location'],
      '', // Assigned To - empty initially
      lead['Pre-Sales Remarks'] || '',
      lead['Date']
    ]);

    // Append to Qualified sheet
    await sheets.spreadsheets.values.append({
      spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
      range: 'Qualified!A:Z',
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      resource: { values: importData }
    });

    res.json({ 
      message: `${newLeads.length} qualified leads imported successfully`,
      importedCount: newLeads.length
    });
  } catch (error) {
    console.error('Import qualified leads error:', error);
    res.status(500).json({ error: 'Failed to import qualified leads' });
  }
});

// 7. Sync Remarks from Salesperson Sheets


// Add endpoint to get leads by User ID
// ✅ Get Leads by User ID (Sanitized & Improved)
app.get('/api/user/:userId/leads', authenticateToken, async (req, res) => {
    const { userId } = req.params;
    const sanitizedUserId = (userId || '').trim();

    try {
        // Fetch users
        const userResponse = await sheets.spreadsheets.values.get({
            spreadsheetId: CONFIG.USERS_SHEET_ID,
            range: 'Users!A:H'
        });
        const users = parseSheetData(userResponse.data.values || []);

        console.log('Logged in User from Token:', req.user); // DEBUG
        console.log('Requested userId in API:', sanitizedUserId);

        const user = users.find(u => (u.ID || '').trim() === sanitizedUserId);

        if (!user) {
            console.error('User not found in Users sheet for ID:', sanitizedUserId);
            return res.status(404).json({ error: `User not found for ID: ${sanitizedUserId}` });
        }

        // ✅ Debug log before restricting
        console.log(`Requestor Role: ${req.user.role}, Requestor ID: ${req.user.id}, Target ID: ${sanitizedUserId}`);

        if (req.user.role === 'salesperson' && req.user.id !== sanitizedUserId) {
            console.warn(`Access denied: Salesperson ${req.user.id} trying to access ${sanitizedUserId}`);
            return res.status(403).json({ 
                error: `Access denied: you can only view your own leads. Your ID: ${req.user.id}, Requested ID: ${sanitizedUserId}` 
            });
        }

        const userSheetName = user.FullName.trim();
        console.log('Fetching leads for user sheet:', userSheetName);

        try {
            const leadResponse = await sheets.spreadsheets.values.get({
                spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
                range: `${userSheetName}!A:Z`
            });

            const leads = parseSheetData(leadResponse.data.values || []);
            return res.json({
                user: user.FullName,
                userId: user.ID,
                leads,
                totalCount: leads.length,
                source: 'user_sheet'
            });

        } catch (sheetError) {
            console.warn(`Sheet for ${userSheetName} not found, checking Qualified sheet.`);

            const qualifiedResponse = await sheets.spreadsheets.values.get({
                spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
                range: 'Qualified!A:Z'
            });

            const allLeads = parseSheetData(qualifiedResponse.data.values || []);
            const userLeads = allLeads.filter(lead =>
                (lead['Assigned To ID'] || '').trim() === sanitizedUserId ||
                (lead['Assigned To'] || '').trim() === user.FullName.trim()
            );

            return res.json({
                user: user.FullName,
                userId: user.ID,
                leads: userLeads,
                totalCount: userLeads.length,
                source: 'qualified_sheet'
            });
        }

    } catch (error) {
        console.error('Get leads by user ID error:', error);
        return res.status(500).json({ error: 'Failed to fetch leads for user' });
    }
});




// Modify sync-remarks to dynamically fetch salesperson names from Users sheet
app.post('/api/sync-remarks', authenticateToken, async (req, res) => {
  try {
    if (req.user.role !== 'manager') {
      return res.status(403).json({ error: 'Access denied' });
    }

    // Fetch all users
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.USERS_SHEET_ID,
      range: 'Users!A:H'
    });
    const values = response.data.values;
    const users = parseSheetData(values);

    // Filter salespersons from users
    const salespersons = users.filter(user => user.Role === 'salesperson').map(user => user.FullName);

    for (const salesperson of salespersons) {
      try {
        const response = await sheets.spreadsheets.values.get({
          spreadsheetId: CONFIG.SALES_TRACKER_SHEET_ID,
          range: `${salesperson}!A:Z`
        });

        const salesData = parseSheetData(response.data.values);
        // TODO: Implement syncing remarks logic
      } catch (error) {
        console.log(`Could not sync ${salesperson} sheet:`, error.message);
      }
    }

    res.json({ message: 'Remarks synced successfully' });
  } catch (error) {
    console.error('Sync remarks error:', error);
    res.status(500).json({ error: 'Failed to sync remarks' });
  }
});

// 8. Get All Users (Manager only)
app.get('/api/users', authenticateToken, async (req, res) => {
  try {
    if (req.user.role !== 'manager') {
      return res.status(403).json({ error: 'Access denied' });
    }

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.USERS_SHEET_ID,
      range: 'Users!A:H'
    });

    const users = parseSheetData(response.data.values);
    
    // Remove password hashes from response
    const sanitizedUsers = users.map(user => ({
      id: user.ID,
      name: user.FullName,
      email: user.Email,
      role: user.Role,
      createdDate: user.CreatedDate,
      status: user['StatusColumn 1']
    }));

    res.json(sanitizedUsers);
  } catch (error) {
    console.error('Get users error:', error);
    res.status(500).json({ error: 'Failed to fetch users' });
  }
});

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Server error:', error);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

export default app;

