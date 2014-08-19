class sqlReplicationStats {
     public static ALL_TABLES = 'All Tables'
     statistics = ko.observable<sqlReplicationStatisticsDto>();
     metrics = ko.observable<sqlReplicaitonMetricsDto>();
     filteredTable = ko.observable<string>(sqlReplicationStats.ALL_TABLES);
     tables: KnockoutComputed<string[]>;
     rateMetrics: KnockoutComputed<metricsDataDto[]>;
     histogramMetrics: KnockoutComputed<metricsDataDto[]>;
     name=ko.observable<string>("");

     
    constructor(replicationStats: sqlReplicationStatsDto) {
        this.statistics(replicationStats.Statistics);
        this.name(replicationStats.Name);
        this.metrics(replicationStats.Metrics);
         this.tables = ko.computed(() => {
             var tableNames = [sqlReplicationStats.ALL_TABLES ];
             var tablesMetrics = this.metrics().TablesMetrics;
             if (!!tablesMetrics) {
                 $.map(tablesMetrics, (value, key) => {
                     tableNames.push(key);
                     return;
                 });
             }
             return tableNames;
         });

         this.rateMetrics = ko.computed(() => {
             var computedRateMetrics = [];
             var generalMetrics = this.metrics().GeneralMetrics;
             var tablesMetrics = this.metrics().TablesMetrics;

             if (!!generalMetrics) {
                 $.map(generalMetrics, (value: metricsDataDto, key: string) => {
                     if (value.Type == "Meter") {
                         value["Name"] = key;
                         computedRateMetrics.push(value);
                     }
                 });
             }

             if (!!tablesMetrics) {
                 $.map(tablesMetrics, (tablesMetricsData: dictionary<metricsDataDto>, tableMetricsKey: string) => {
                     if ((!this.filteredTable() || this.filteredTable() == sqlReplicationStats.ALL_TABLES) || tableMetricsKey == this.filteredTable()) {
                         $.map(tablesMetricsData, (value, key) => {
                             if (value.Type == "Meter") {
                                 var newMetric = value;
                                 newMetric["Name"] = tableMetricsKey + "." + key;
                                 computedRateMetrics.push(newMetric);
                             }
                         });
                     }
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
                     if (value.Type == "Historgram") {
                         value["Name"] = key;

                         if (!!value["Percentiles"]) {
                             value["Percentiles"] = $.map(value["Percentiles"], (percentileValue, percentileName) => {
                                 return "[" + percentileName + ":" + percentileValue.toFixed(2)+"]";
                             }).join(";");
                         }
                         computedHistogramMetrics.push(value);
                     }
                 });
             }

             if (!!tablesMetrics) {
                 $.map(tablesMetrics, (tablesMetricsData: dictionary<metricsDataDto>, tableMetricsKey: string) => {
                     if ((!this.filteredTable() || this.filteredTable() == sqlReplicationStats.ALL_TABLES) || tableMetricsKey == this.filteredTable()) {
                         $.map(tablesMetricsData, (value, key) => {
                             if (value.Type == "Historgram") {
                                 var newMetric = value;
                                 newMetric["Name"] = tableMetricsKey + "." + key;
                                 computedHistogramMetrics.push(newMetric);

                                 if (!!newMetric["Percentiles"]) {
                                     newMetric["Percentiles"] = $.map(newMetric["Percentiles"], (percentileValue, percentileName) => {
                                         return "[" + percentileName + ":" + percentileValue.toFixed(2)+"]";
                                     }).join(";");
                                 }

                             }
                         });
                     }
                 });
             }
             return computedHistogramMetrics;
         });

             
     }

}

export =sqlReplicationStats;