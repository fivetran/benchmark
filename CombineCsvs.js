/*
 * Combine gs://fivetran-benchmark/... CSVs into fewer, larger files
 * 
 * In theory it would be better to just get Hive to output larger files in the first place,
 * but I couldn't figure out how to do that and this was easy.
 * 
 * Run with Node.
 */

const CP = require('child_process');

function exec(cmd) {
    return CP.execSync(cmd, {encoding: 'utf8'})
        .split(/\n/)
        .filter(line => line != '');
}

function partition(arr, n) {
    let result = [];

    for (var i = 0; i < arr.length; i += n) {
        let next = [];

        for (var j = i; j < arr.length && j < i + n; j++) {
            next.push(arr[j]);
        }

        result.push(next);
    }

    return result;
}

let dirs = exec(`gsutil ls gs://fivetran-benchmark/tpcds/csv/`, {encoding:'utf8'}).slice(1);

for (let dir of dirs) {
    let files = exec(`gsutil ls ${dir}`).slice(1);
    let chunks = partition(files, 8);
    let path = dir.split('/');
    let table = path[path.length - 2];

    for (var i = 0; i < chunks.length; i++) {
        let chunk = chunks[i];

        exec(`gsutil compose ${chunk.join(' ')} gs://fivetran-benchmark/tpcds/csv_large/${table}/${i}`);
    }
}