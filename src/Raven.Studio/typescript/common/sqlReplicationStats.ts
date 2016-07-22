import genUtils = require("common/generalUtils");

class sqlReplicationStats {
     public static ALL_TABLES = 'All Tables'
     statistics = ko.observable<sqlReplicationStatisticsDto>();
     metrics = ko.observable<sqlReplicaitonMetricsDto>();
     rateMetrics: KnockoutComputed<metricsDataDto[]>;
     histogramMetrics: KnockoutComputed<metricsDataDto[]>;
     name=ko.observable<string>("");

     
    constructor(replicationStats: sqlReplicationStatsDto) {
        if (replicationStats.Statistics && replicationStats.Statistics.LastErrorTime && replicationStats.Statistics.LastErrorTime === "0001-01-01T00:00:00.0000000") {
            replicationStats.Statistics.LastErrorTime = "No Errors";
        }
        this.statistics(replicationStats.Statistics);
        this.name(replicationStats.Name);
        this.metrics(replicationStats.Metrics);
        this.rateMetrics = ko.computed(() => {
             var computedRateMetrics = [];
             var generalMetrics = this.metrics().GeneralMetrics;
             var tablesMetrics = this.metrics().TablesMetrics;

             if (!!generalMetrics) {
                 $.map(generalMetrics, (value: metricsDataDto, key: string) => {
                     if (value.Type == "Meter") {
                         value["Name"] = key;
                         value["Table"] = "";
                         $.map(value, (propertyValue, propertyName) => {
                             if (!isNaN(propertyValue)) {
                                 value[propertyName] = genUtils.formatAsCommaSeperatedString(propertyValue, 2);
                             }
                         });
                         computedRateMetrics.push(value);
                     }
                 });
             }

             if (!!tablesMetrics) {
                 $.map(tablesMetrics, (tablesMetricsData: dictionary<metricsDataDto>, tableMetricsKey: string) => {
                    $.map(tablesMetricsData, (value, key) => {
                        if (value.Type === "Meter") {
                            var newMetric = value;
                            newMetric["Name"] = key;
                            value["Table"] = tableMetricsKey;
                            $.map(value, (propertyValue, propertyName) => {
                                if (!isNaN(propertyValue)) {
                                    value[propertyName] = genUtils.formatAsCommaSeperatedString(propertyValue, 2);
                                }
                            });
                            computedRateMetrics.push(newMetric);
                        }
                    });
                     
                 });
             }
             return computedRateMetrics;
         });

         this.histogramMetrics = ko.computed(() => {
             var computedHistogramMetrics = [];
             var generalMetrics = this.metrics().GeneralMetrics;
             var tablesMetrics = this.metrics().TablesMetrics;

             if (!!generalMetrics) {
                 $.map(generalMetrics, (value: metricsDataDto, key: string) => {
                     if (value.Type === "Histogram") {
                         value["Name"] = key;
                         value["Table"] = "";
                         value["Percentiles"] = $.map(value["Percentiles"], (percentileValue, percentile) => {
                             return {
                                 percentileValue: genUtils.formatAsCommaSeperatedString(percentileValue, 2),
                                 percentile: percentile
                             }
                         });
                         $.map(value, (propertyValue, propertyName) => {
                             if (!isNaN(propertyValue)) {
                                 value[propertyName] = genUtils.formatAsCommaSeperatedString(propertyValue, 2);
                             }
                         });
                         computedHistogramMetrics.push(value);
                     }
                 });
             }

             if (!!tablesMetrics) {
                 $.map(tablesMetrics, (tablesMetricsData: dictionary<metricsDataDto>, tableMetricsKey: string) => {
                    $.map(tablesMetricsData, (value, key) => {
                        if (value.Type == "Histogram") {
                            value["Name"] = key;
                            value["Table"] = tableMetricsKey ;
                            value["Percentiles"] = $.map(value["Percentiles"], (percentileValue, percentile) => {
                             return {
                                    percentileValue: genUtils.formatAsCommaSeperatedString(percentileValue, 2),
                                    percentile: percentile
                                }
                         });
                            $.map(value, (propertyValue, propertyName) => {
                                if (!isNaN(propertyValue)) {
                                    value[propertyName] = genUtils.formatAsCommaSeperatedString(propertyValue, 2);
                                }
                            });
                            computedHistogramMetrics.push(value);
                        }
                    });
                 });
             }
             return computedHistogramMetrics;
         });
     }
}

export =sqlReplicationStats;
