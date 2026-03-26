// Temporary script to explore VestiLake ODBC_Quotes table structure
const https = require('https');
const qs = require('querystring');

const RT = process.env.FABRIC_REFRESH_TOKEN || '';
const TID = process.env.FABRIC_TENANT_ID || '';
const LAKE_WS = '2929476c-7b92-4366-9236-ccd13ffbd917';

function req(opts, body) {
    return new Promise((res, rej) => {
        const r = https.request(opts, resp => {
            const c = []; resp.on('data', d => c.push(d));
            resp.on('end', () => res({ status: resp.statusCode, body: Buffer.concat(c).toString() }));
        });
        r.on('error', rej); if (body) r.write(body); r.end();
    });
}

async function main() {
    // Get token
    const pb = qs.stringify({ client_id: '1950a258-227b-4e31-a9cf-717495945fc2', grant_type: 'refresh_token', refresh_token: RT, scope: 'https://analysis.windows.net/powerbi/api/.default offline_access' });
    const tr = await req({ hostname: 'login.microsoftonline.com', path: '/' + TID + '/oauth2/v2.0/token', method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(pb) } }, pb);
    const td = JSON.parse(tr.body);
    if (!td.access_token) { console.log('Token failed:', td.error_description || td.error); return; }
    const token = td.access_token;
    console.log('Authenticated.\n');

    // List datasets in VestiLake workspace
    const ds = await req({ hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + LAKE_WS + '/datasets', method: 'GET', headers: { 'Authorization': 'Bearer ' + token } });
    console.log('=== Datasets in VestiLake workspace ===');
    const datasets = JSON.parse(ds.body);
    (datasets.value || []).forEach(d => console.log(' - ' + d.name + ' | ID: ' + d.id + ' | configuredBy: ' + d.configuredBy));

    // Try to query each dataset for ODBC_Quotes
    for (const d of (datasets.value || [])) {
        console.log('\n--- Trying dataset: ' + d.name + ' (' + d.id + ') ---');
        // Try a simple DAX query to get top 3 rows
        const bodyStr = JSON.stringify({
            queries: [{ query: "EVALUATE TOPN(3, 'ODBC_Quotes')" }],
            serializerSettings: { includeNulls: true },
        });
        const r = await req({
            hostname: 'api.powerbi.com',
            path: '/v1.0/myorg/groups/' + LAKE_WS + '/datasets/' + d.id + '/executeQueries',
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) },
        }, bodyStr);
        if (r.status === 200) {
            const data = JSON.parse(r.body);
            if (data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0]) {
                const rows = data.results[0].tables[0].rows;
                console.log('SUCCESS! Found ODBC_Quotes. Columns:');
                if (rows.length > 0) {
                    Object.keys(rows[0]).forEach(k => console.log('  ' + k + ' = ' + JSON.stringify(rows[0][k]).substring(0, 80)));
                }
                console.log('Sample rows: ' + rows.length);
            } else if (data.error) {
                console.log('DAX error: ' + JSON.stringify(data.error).substring(0, 200));
            }
        } else {
            console.log('HTTP ' + r.status + ': ' + r.body.substring(0, 200));
        }
    }

    // Also try listing tables via Lakehouse API
    console.log('\n=== Lakehouse tables ===');
    const lt = await req({ hostname: 'api.fabric.microsoft.com', path: '/v1/workspaces/' + LAKE_WS + '/lakehouses/21b85aa7-d4d3-4221-9365-ea024dc2461a/tables', method: 'GET', headers: { 'Authorization': 'Bearer ' + token } });
    console.log('Status:', lt.status);
    if (lt.status === 200) {
        const tables = JSON.parse(lt.body);
        (tables.data || tables.value || []).forEach(t => console.log(' - ' + (t.name || t.tableName || JSON.stringify(t))));
    } else {
        console.log(lt.body.substring(0, 300));
    }
}
main().catch(e => console.error(e));
