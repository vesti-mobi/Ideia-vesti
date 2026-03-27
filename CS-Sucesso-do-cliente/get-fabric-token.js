/**
 * Obtém refresh token do Microsoft Fabric via Device Code Flow.
 * Uso: node get-fabric-token.js
 */
const https = require('https');
const fs = require('fs');
const path = require('path');

const TENANT_ID = 'ea649dfc-28b2-42fc-98bb-79f264959504';
const CLIENT_ID = '14d82eec-204b-4c2f-b7e8-296a70dab67e'; // Microsoft Graph PowerShell (public)
const SCOPE = 'https://database.windows.net//.default offline_access';

function post(hostname, path, body) {
    return new Promise((resolve, reject) => {
        const data = typeof body === 'string' ? body : require('querystring').stringify(body);
        const req = https.request({
            hostname, path, method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(data) },
        }, res => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch(e) { reject(e); } });
        });
        req.on('error', reject);
        req.write(data);
        req.end();
    });
}

async function main() {
    // Step 1: Request device code
    console.log('Solicitando código de dispositivo...\n');
    const deviceRes = await post('login.microsoftonline.com', `/${TENANT_ID}/oauth2/v2.0/devicecode`, {
        client_id: CLIENT_ID,
        scope: SCOPE,
    });

    if (deviceRes.error) {
        console.error('Erro:', deviceRes.error_description);
        process.exit(1);
    }

    console.log('================================================');
    console.log(deviceRes.message);
    console.log('================================================\n');

    // Open browser automatically
    require('child_process').exec(`start "" "${deviceRes.verification_uri}"`);

    // Step 2: Poll for token
    const interval = (deviceRes.interval || 5) * 1000;
    const expires = Date.now() + (deviceRes.expires_in || 900) * 1000;

    console.log('Aguardando login...');
    while (Date.now() < expires) {
        await new Promise(r => setTimeout(r, interval));
        const tokenRes = await post('login.microsoftonline.com', `/${TENANT_ID}/oauth2/v2.0/token`, {
            client_id: CLIENT_ID,
            grant_type: 'urn:ietf:params:oauth:grant-type:device_code',
            device_code: deviceRes.device_code,
        });

        if (tokenRes.access_token) {
            // Save to .env
            const envPath = path.join(__dirname, '.env');
            let env = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf-8') : '';

            const updates = {
                'FABRIC_TENANT_ID': TENANT_ID,
                'FABRIC_REFRESH_TOKEN': tokenRes.refresh_token,
                'FABRIC_CLIENT_ID': CLIENT_ID,
            };
            for (const [key, val] of Object.entries(updates)) {
                const regex = new RegExp(`^${key}=.*$`, 'm');
                if (regex.test(env)) {
                    env = env.replace(regex, `${key}=${val}`);
                } else {
                    env = env.trimEnd() + `\n${key}=${val}`;
                }
            }
            fs.writeFileSync(envPath, env, 'utf-8');

            console.log('\n=== SUCESSO ===');
            console.log('Tokens salvos em .env');
            console.log('Agora rode: node build-data.js');
            process.exit(0);
        }

        if (tokenRes.error && tokenRes.error !== 'authorization_pending') {
            console.error('\nErro:', tokenRes.error_description);
            process.exit(1);
        }
        process.stdout.write('.');
    }
    console.log('\nTimeout - tente novamente.');
    process.exit(1);
}

main().catch(e => { console.error(e); process.exit(1); });
