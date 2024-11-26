module.exports = {
  description: 'An overview of data in the timeline',
  include_date: true,
  label: 'Person Count By Month',
  template: 'primary',
  data_sources: {
    default: {
      table: 'person',
      date_field: 'ts',
    },
  },
  components: {
    a_title: 'Count of People by Month Created',
    a0: {
      component: 'Engine9BarChart',
      is_date: true,
      dimension: { label: 'Month', eql: 'MONTH(created_at)' },
      metrics: [{ label: 'People', eql: 'count(*)' }],
      conditions: [],
    },
    a1: {
      table: 'dual',
      component: 'Engine9Scorecard',
      metrics: [{ label: 'sleep_1', eql: 'sleep(1)' }],
    },
    a2: {
      table: 'dual',
      component: 'Engine9Scorecard',
      metrics: [{ label: 'sleep_2', eql: 'sleep(1)' }],
      conditions: [],
    },
  },
};
