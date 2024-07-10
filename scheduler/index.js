require('dotenv').config({ path: '../.env' });
const Scheduler = require('./SQLJobScheduler');

async function start() {
  const scheduler = new Scheduler();
  await scheduler.init();
  scheduler.toManagerQueue.push({ eventType: 'ping' });
  scheduler.poll();
}

start();
