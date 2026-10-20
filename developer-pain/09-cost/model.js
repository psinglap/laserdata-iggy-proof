// Apache Iggy TCO model, by LaserData. Pure function, no hidden multipliers:
// every number comes from an input the user can see and change.
(function (root) {
  // AWS us-east-1, Linux, on-demand list prices, checked 2026-10-19.
  // Sources: aws.amazon.com/ec2/pricing/on-demand, aws.amazon.com/ebs/pricing,
  // aws.amazon.com/s3/pricing, aws.amazon.com/ec2/pricing/on-demand (data transfer).
  var INSTANCES = [
    { id: "i4i.xlarge",   vcpu: 4,  ramGiB: 32,  nvmeGB: 937,  hourly: 0.343 },
    { id: "i4i.2xlarge",  vcpu: 8,  ramGiB: 64,  nvmeGB: 1875, hourly: 0.686 },
    { id: "i4i.4xlarge",  vcpu: 16, ramGiB: 128, nvmeGB: 3750, hourly: 1.373 },
    { id: "i7ie.xlarge",  vcpu: 4,  ramGiB: 32,  nvmeGB: 2500, hourly: 0.5198 },
    { id: "im4gn.xlarge", vcpu: 4,  ramGiB: 16,  nvmeGB: 1875, hourly: 0.36379 },
    { id: "is4gen.xlarge",vcpu: 4,  ramGiB: 24,  nvmeGB: 3750, hourly: 0.5763 },
    { id: "m7i.xlarge",   vcpu: 4,  ramGiB: 16,  nvmeGB: 0,    hourly: 0.2016 },
    { id: "m7i.2xlarge",  vcpu: 8,  ramGiB: 32,  nvmeGB: 0,    hourly: 0.4032 },
    { id: "m7g.2xlarge",  vcpu: 8,  ramGiB: 32,  nvmeGB: 0,    hourly: 0.3264 }
  ];
  var PRICES = { ebsGp3PerGBMonth: 0.08, crossAzPerGB: 0.02, s3PerGBMonth: 0.023 };
  var HOURS_PER_MONTH = 730, SECONDS_PER_DAY = 86400;

  // in: writeMBps, fanout, retentionDays, replicas, zones, instance {nvmeGB,hourly},
  //     headroomPct, crossZoneClientPct, measuredMBpsPerNode (null = not measured), prices
  function estimate(i) {
    var p = i.prices || PRICES;
    var storedGB = i.writeMBps * SECONDS_PER_DAY * i.retentionDays * i.replicas / 1000;
    var fill = 1 - i.headroomPct / 100;
    var local = i.instance.nvmeGB > 0;
    var byStorage = local ? Math.ceil(storedGB / (i.instance.nvmeGB * fill)) : 0;
    var byThroughput = i.measuredMBpsPerNode ? Math.ceil(i.writeMBps / i.measuredMBpsPerNode) : 0;
    var nodes = Math.max(i.replicas, byStorage, byThroughput);
    var binding = nodes === byThroughput && byThroughput > 0 ? "throughput"
                : nodes === byStorage && byStorage > i.replicas ? "storage" : "replicas";
    var compute = nodes * i.instance.hourly * HOURS_PER_MONTH;
    var storage = local ? 0 : (storedGB / fill) * p.ebsGp3PerGBMonth;
    var monthGB = i.writeMBps * SECONDS_PER_DAY * 30.4 / 1000;
    var replGB = i.zones > 1 ? monthGB * (i.replicas - 1) : 0;
    var clientGB = i.zones > 1 ? monthGB * (1 + i.fanout) * i.crossZoneClientPct / 100 : 0;
    var network = (replGB + clientGB) * p.crossAzPerGB;
    return {
      storedGB: storedGB, nodes: nodes, binding: binding, throughputChecked: !!i.measuredMBpsPerNode,
      compute: compute, storage: storage, network: network, total: compute + storage + network,
      // Reference only: Apache Iggy has no tiered storage today.
      objectStoreRef: (storedGB / i.replicas) * p.s3PerGBMonth
    };
  }
  var api = { INSTANCES: INSTANCES, PRICES: PRICES, estimate: estimate };
  if (typeof module !== "undefined") module.exports = api; else root.IggyTCO = api;
})(this);
