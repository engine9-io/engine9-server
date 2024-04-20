const PacketWorker = require('../../workers/PacketWorker');

async function test(opts) {
  const schemaWorker = new PacketWorker(opts);
  const packetPath = `${__dirname}/foo.zip`;
  const directory = await schemaWorker.list({ packet_path: packetPath });
  debugger;
  return directory;
}

test({});
