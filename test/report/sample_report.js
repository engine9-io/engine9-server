{
  const o = {
    data_sources: {
      default: {
        table: 'global_message_summary',
        date_field: 'publish_date',
        conditions: [{ eql: "channel='email'" }],
      },
    },
    include_date: true,
    label: 'Email Engagement',
    description: 'Key performance stats including send count, open & click rates, and unsubscribes.',
    tags: ['Email'],
    subcategory: 'Email',
    components: {
      a_title: 'Email Key Metrics',
      a1: {
        component: 'FraktureScorecard',
        label: 'Messages',
        metric: { eql: 'count(distinct message_id)' },
      },
      a2: {
        component: 'FraktureScorecard',
        label: 'Total Sent',
        metric: { eql: 'sum(sent)' },
      },
      a3: {
        component: 'FraktureScorecard',
        label: 'Open Rate',
        metrics: [
          { eql: 'sum(impressions)/sum(sent)', format: 'percent' },
          { eql: 'sum(impressions)' },
        ],
      },
      a4: {
        component: 'FraktureScorecard',
        label: 'Click Rate',
        metrics: [{ eql: 'sum(clicks)/sum(sent)', format: 'percent' }, { eql: 'sum(clicks)' }],
      },
      a5: {
        component: 'FraktureScorecard',
        label: 'Unsubscribes',
        metrics: [{ eql: 'sum(unsubscribes)' },
        ],
      },
      a6: {
        component: 'FraktureScorecard',
        label: 'Bounces',
        metrics: [{ eql: 'sum(soft_bounces+hard_bounces)' }],
      },
      b_title: 'Opens and Clicks By Date',
      b1: {
        component: 'FraktureBarChart',
        is_date: true,
        dimension: { eql: 'publish_date' },
        metrics: [
          { label: 'Opened', eql: 'sum(impressions)', yaxis: 'right' },
          { label: 'Clicked', eql: 'sum(clicks)', yaxis: 'left' },
        ],
        sort: { eql: 'publish_date' },
      },
      c_title: 'Message Details',
      c1: {
        component: 'FraktureReportTable',
        metrics: [
          { label: 'Subject', eql: 'subject', col: 0 },
          { label: 'Message', eql: 'label', col: 0 },
          { label: 'Source Code', eql: 'final_primary_source_code', col: 0 },
          {
            label: 'Send Date', eql: 'publish_date', col: 1, format: 'date',
          },
          {
            label: 'Total Sent', eql: 'sent', col: 2, format: 'number',
          },
          {
            label: 'Open Rate', eql: 'impressions/sent', format: 'percent', col: 3,
          },
          { label: 'Opened', eql: 'impressions', col: 3 },
          {
            label: 'Click Rate', eql: 'clicks/sent', format: 'percent', col: 4,
          },
          { label: 'Clicked', eql: 'clicks', col: 4 },
          {
            label: 'Unsub Rate', eql: 'unsubscribes/sent', format: 'percent', col: 5,
          },
          { label: 'Unsubscribed', eql: 'unsubscribes', col: 5 },
          {
            label: 'Bounce Rate', eql: 'soft_bounces+hard_bounces/sent', format: 'percent', col: 6,
          },
          { label: 'Bounces', eql: 'soft_bounces+hard_bounces', col: 6 },
        ],
        orderBy: { eql: 'publish_date', orderBy_direction: 'DESC' },
      },
      d1: {
        component: 'FraktureReportTable',
        label: 'Campaign Summary',
        dimensions: [
          { label: 'Campaign', eql: 'campaign_name' },
        ],
        metrics: [
          { label: 'Total Sent', eql: 'sum(sent)' },
          { label: 'Opened', eql: 'sum(impressions)' },
          { label: 'Open Rate', eql: 'sum(impressions)/sum(sent)', format: 'percent' },
          { label: 'Clicked', eql: 'sum(clicks)' },
          { label: 'Click Rate', eql: 'sum(clicks)/sum(sent)', format: 'percent' },
          { label: 'Unsubscribed', eql: 'sum(unsubscribes)' },
          { label: 'Unsub Rate', eql: 'sum(unsubscribes)/sum(sent)', format: 'percent' },
          { label: 'Bounces', eql: 'sum(soft_bounces+hard_bounces)' },
          { label: 'Bounce Rate', eql: 'sum(soft_bounces+hard_bounces)/sum(sent)', format: 'percent' },
        ],
      },
    },
  };
  module.exports = o;
}
