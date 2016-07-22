import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import moment = require("moment");
import d3 = require('d3');
import nv = require('nvd3');

class indexPrefetches extends viewModelBase {
    /*
    currentStats: KnockoutObservable<databaseStatisticsDto> = ko.observable(null);

    prefetchesChart: any = null;
    prefetchesChartData: any = [{
        key: 'Prefetches',
        values: []
    }];
    static prefetchesAllowZoom = false;

    attached() {
        super.attached();
        indexPrefetches.prefetchesAllowZoom = false;
    }

    compositionComplete() {
        this.modelPolling();
    }

    canDeactivate() {
        $(window).off('resize.indexPrefetches');
        return true;
    }

    modelPolling() {
        return this.fetchStats().then(() => {
            this.appendData();
            this.updateChart();
        });
    }

    appendData() {
        var stats = this.currentStats();
        var prefeches = stats.Prefetches;
        var values = this.prefetchesChartData[0].values;

        for (var i = 0; i < prefeches.length; i++) {
            var item = {
                x: new Date(prefeches[i].Timestamp).getTime(),
                size: moment.duration(prefeches[i].Duration).asMilliseconds(),
                y: prefeches[i].Size,
                payload: prefeches[i]
            };

            var match = values.first(e => e.x === item.x && e.y === item.y);
            if (!match) {
                values.push(item);
            }
        }
    }


    updateChart() {
        
        if (!this.prefetchesChart) {
            nv.addGraph(function () {
                var chart = nv.models.timelinesChart()
                    .showDistX(true)
                    .showDistY(true)
                    .showControls(true)
                    .color(d3.scale.category10().range())
                    .transitionDuration(250)
                ;
                chart.yAxis.showMaxMin(false).axisLabel('size').tickFormat(d3.format(',f'));

                chart.forceY([0]);
                chart.y2Axis.showMaxMin(false);
                chart.xAxis.showMaxMin(false);
                chart.x2Axis.showMaxMin(false);
                chart.xAxis.tickFormat(function (_) { return d3.time.format("%H:%M:%S")(new Date(_)); });
                chart.x2Axis.tickFormat(function (_) { return d3.time.format("%H:%M:%S")(new Date(_)); });

                
                chart.tooltipContent(function (key, x, y, data) {
                    var ff = d3.format(",f");
                    return '<h4>' + key + '</h4>'
                        + '<strong>Timestamp:</strong> ' + data.point.payload.Timestamp + '<br />'
                        + '<strong>Duration:</strong> ' + data.point.payload.Duration + '<br />'
                        + '<strong>Size:</strong> ' + ff(data.point.payload.Size) + '<br />'
                        + '<strong>Retries:</strong> ' + ff(data.point.payload.Retries);
                });

                chart.dispatch.on('controlsChange', function (e) {
                    indexPrefetches.prefetchesAllowZoom = !!e.disabled;
                });

                $(window).on('resize.indexPrefetches', (e) => chart(e));

                return chart;
            }, (chart) => {
                this.prefetchesChart = chart;
                d3.select('#prefetchesContainer svg')
                    .datum(this.prefetchesChartData)
                    .call(this.prefetchesChart);
                });
        } else {
            if (!indexPrefetches.prefetchesAllowZoom) {
                d3.select('#prefetchesContainer svg')
                    .datum(this.prefetchesChartData)
                    .call(this.prefetchesChart);
            }
        }
    }

    
    fetchStats(): JQueryPromise<databaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getDatabaseStatsCommand(db)
                .execute().done((s: databaseStatisticsDto) => this.currentStats(s));
        }
        return null;
    }
    */
}

export = indexPrefetches; 
