const fs = require('node:fs');

const fsp = fs.promises;
const FileWorker = require('../workers/FileWorker');

const fworker = new FileWorker();

/*
  mrss_tpl/stored_input/2025/<bot_id>/advocacy/
  9f71c686-b948-5656-8d88-b92acd1e2951/2025_02_27_12_12_53_492_1231.input.csv.gz
*/

async function cleanPath(p) {
  if (p.indexOf('statistics.json') >= 0) return `aws s3 rm s3://engine9-accounts/${p}`;
  const parts = p.split('/');
  if (p.indexOf(':') > 0) {
    const newPath = parts.filter((d) => !d.match(/20[0-5]{2}/)).join('/');
    return `aws s3 mv s3://engine9-accounts/${p} s3://engine9-accounts/${newPath}`;
  }

  try {
    const metadata = await fworker.json({ filename: `s3://engine9-accounts/${parts.slice(0, -1).concat('metadata.json').join('/')}` });

    return `#${JSON.stringify(metadata)}\naws s3 mv s3://engine9-accounts/${p} s3://engine9-accounts/${'ffffff'}`;
  } catch (e) {
    return `#ignoring ${p}`;
  }
}

if (require.main === module) {
  (async function () {
    const paths = (await fsp.readFile(process.argv[2])).toString().split('\n').map((d) => d.trim()).filter(Boolean);
    paths.forEach(async (p) => {
      const clean = await cleanPath(p);
      // eslint-disable-next-line no-console
      if (clean) console.log(clean);
    });
  }());
}
