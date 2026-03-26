const https = require('https');
const qs = require('querystring');
const { Connection, Request } = require('tedious');
const RT = process.env.FABRIC_REFRESH_TOKEN || '';
const TID = process.env.FABRIC_TENANT_ID || '';
function req(opts, body) { return new Promise((res, rej) => { const r = https.request(opts, resp => { const c = []; resp.on('data', d => c.push(d)); resp.on('end', () => res({ status: resp.statusCode, body: Buffer.concat(c).toString() })); }); r.on('error', rej); if (body) r.write(body); r.end(); }); }
function runSQL(token, query, label) { return new Promise((resolve, reject) => { console.log('  ' + label + '...'); const conn = new Connection({ server: '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com', authentication: { type: 'azure-active-directory-access-token', options: { token } }, options: { database: 'VestiHouse', encrypt: true, port: 1433, requestTimeout: 120000 } }); const rows = []; conn.on('connect', err => { if (err) { reject(err); return; } const request = new Request(query, err => { if (err) reject(err); conn.close(); }); request.on('row', columns => { const row = {}; columns.forEach(col => { row[col.metadata.colName] = col.value; }); rows.push(row); }); request.on('requestCompleted', () => { console.log('  ' + label + ': ' + rows.length + ' rows'); resolve(rows); }); conn.execSql(request); }); conn.connect(); }); }

async function main() {
    const pb = qs.stringify({ client_id: '1950a258-227b-4e31-a9cf-717495945fc2', grant_type: 'refresh_token', refresh_token: RT, scope: 'https://database.windows.net/.default offline_access' });
    const tr = await req({ hostname: 'login.microsoftonline.com', path: '/' + TID + '/oauth2/v2.0/token', method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(pb) } }, pb);
    const token = JSON.parse(tr.body).access_token;

    // Check distinct status_payment values and counts
    console.log('=== ODBC_Quotes status_payment values ===');
    const sp = await runSQL(token, "SELECT status_payment, COUNT(*) as cnt FROM dbo.ODBC_Quotes GROUP BY status_payment ORDER BY cnt DESC", 'status_payment');
    sp.forEach(r => console.log('  "' + r.status_payment + '" -> ' + r.cnt));

    // Check distinct status values
    console.log('\n=== ODBC_Quotes status (numeric) values ===');
    const st = await runSQL(token, "SELECT status, COUNT(*) as cnt FROM dbo.ODBC_Quotes GROUP BY status ORDER BY cnt DESC", 'status');
    st.forEach(r => console.log('  ' + r.status + ' -> ' + r.cnt));

    // Check Anterior2023 - does it have status columns?
    console.log('\n=== OBDC_Quotes_Anterior2023 all columns ===');
    const cols = await runSQL(token, "SELECT TOP 1 * FROM dbo.OBDC_Quotes_Anterior2023", 'cols');
    if (cols[0]) Object.entries(cols[0]).forEach(([k, v]) => console.log('  ' + k + ' = ' + JSON.stringify(v).substring(0, 80)));

    // Check status field in Anterior2023 if exists
    console.log('\n=== Anterior2023 - check for status fields ===');
    try {
        const sp2 = await runSQL(token, "SELECT TOP 5 status_payment FROM dbo.OBDC_Quotes_Anterior2023", 'anterior status_payment');
        sp2.forEach(r => console.log('  ' + JSON.stringify(r)));
    } catch(e) { console.log('  No status_payment column'); }
    try {
        const st2 = await runSQL(token, "SELECT TOP 5 status FROM dbo.OBDC_Quotes_Anterior2023", 'anterior status');
        st2.forEach(r => console.log('  ' + JSON.stringify(r)));
    } catch(e) { console.log('  No status column'); }
}
main().catch(e => console.error('FATAL:', e));
