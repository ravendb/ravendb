import viewModelBase = require("viewmodels/viewModelBase");
import app = require("durandal/app");
import tempStatDialog = require("viewmodels/database/status/indexing/tempStatDialog");
import getIndexesPerformance = require("commands/database/debug/getIndexesPerformance");
import getIndexStatsCommand = require("commands/database/index/getIndexStatsCommand");

class metrics extends viewModelBase { 

    data: Raven.Client.Data.Indexes.IndexPerformanceStats[] = [];
    currentBatches = new Map<string, Raven.Client.Data.Indexes.IndexingPerformanceBasicStats>();

    private isoParser = d3.time.format.iso;

    private static readonly barHeight = 10;
    private static readonly maxRecurseLevel = 4;
    private static readonly pixelsPerSecond = 2;
    private static readonly innerGroupPadding = 10;
    private static readonly verticalPadding = 10;
    private static readonly legendPadding = 200;
    private static readonly singleIndexGroupHeight = metrics.barHeight + 2 * metrics.innerGroupPadding * metrics.maxRecurseLevel;

    private svg: d3.Selection<Raven.Client.Data.Indexes.IndexPerformanceStats[]>;
    private xScale: d3.time.Scale<number, number>;
    private xAxis: d3.svg.Axis;

    private yScale: d3.scale.Ordinal<string, number>;
    private yAxis: d3.svg.Axis;

    private colorScale = d3.scale.category20();

    private xTickFormat = d3.time.format("%H:%M:%S");

    activate(args: any): JQueryPromise<any> {
        super.activate(args);
        const perfTask = new getIndexesPerformance(this.activeDatabase())
            .execute()
            .done(result => this.data = result);

        return perfTask.then(() => {
            return new getIndexStatsCommand(this.activeDatabase())
                .execute()
                .done(result => {
                    this.extractCurrentlyRunning(result);
                });
        });
    }

    attached() {
        super.attached();
        this.svg = d3.select("#indexPerformanceGraph");
    }

    compositionComplete() {
        super.compositionComplete();

        this.draw();
    }

    private extractCurrentlyRunning(indexStats: Array<Raven.Client.Data.Indexes.IndexStats>) {
        indexStats.forEach(stat => {
            const indexName = stat.Name;
            if (stat.LastBatchStats) {
                const lastBatchStats = stat.LastBatchStats;
                const startTime = lastBatchStats.Started;

                // try to find duplicate in performance
                let duplicateFound = false;

                for (let i = 0; i < this.data.length; i++) {
                    const currentPerf = this.data[i];
                    if (currentPerf.IndexName === indexName) {
                        for (let j = 0; j < currentPerf.Performance.length; j++) {
                            if (currentPerf.Performance[j].Started === startTime) {
                                duplicateFound = true;
                                break;
                            }
                        }

                        break;
                    }
                }

                if (duplicateFound) {
                    return;
                }

                this.currentBatches.set(indexName, lastBatchStats);
            }
        });
    }

    private draw() {
        const self = this;

        if (this.data.length === 0) {
            return;
        }

        const indexNames = self.findIndexNames();
        const [minTime, maxTime] = self.findTimeRanges();

        const timeExtent = maxTime.getTime() - minTime.getTime();

        const paddingTop = 40;

        const totalWidth = metrics.extent(timeExtent);

        self.xScale = d3.time.scale<number>()
            .range([0, totalWidth])
            .domain([minTime, maxTime]);

        self.svg.append("g")
            .attr("class", "x axis");

        const ticks = d3.scale.linear()
            .domain([0, timeExtent])
            .ticks(Math.ceil(timeExtent / 10000)).map(y => self.xScale.invert(y));

        this.xAxis = d3.svg.axis()
            .scale(self.xScale)
            .orient("top")
            .tickValues(ticks)
            .tickSize(10)
            .tickFormat(self.xTickFormat);

        const totalHeight = indexNames.length * (metrics.singleIndexGroupHeight + metrics.verticalPadding);

        this.svg.select(".x.axis")
            .attr("transform", "translate(" + metrics.legendPadding + "," + paddingTop + ")")
            .call(self.xAxis);

        this.yScale = d3.scale.ordinal()
            .domain(indexNames)
            .rangeBands([paddingTop, paddingTop + totalHeight]);

        this.yAxis = d3.svg.axis()
            .scale(this.yScale)
            .orient("left");

        this.svg.append('g')
            .attr('class', 'y axis map')
            .attr("transform", "translate(" + metrics.legendPadding + ",0)");

        self.svg.select(".y.axis.map")
            .call(self.yAxis);

        $("#indexPerformanceGraph")
            .width(300 + totalWidth)
            .height(totalHeight + 100);

        const graphData = self.svg.append("g")
            .attr('class', 'graph_data')
            .attr('transform', "translate(" + metrics.legendPadding + ",0)");

        this.data.forEach(perfStat => {
            this.graphForIndex(perfStat, graphData);
        });

        this.currentBatches.forEach((stats, index) => {
            this.graphCurrentStats(stats, index, graphData);
        });
    }

    private graphCurrentStats(stats: Raven.Client.Data.Indexes.IndexingPerformanceBasicStats,
        indexName: string,
        container: d3.Selection<any>) {

        const self = this;
        const startTime = self.isoParser.parse(stats.Started);
        const endTime = new Date(startTime.getTime() + stats.DurationInMilliseconds);

        const inProgressPerfGroup = container.append("g")
            .attr('class', 'in_progress_group_item')
            .attr("transform", "translate(" + self.xScale(startTime) + "," + (self.yScale(indexName) + metrics.verticalPadding / 2) + ")");

        const rect = inProgressPerfGroup.append("rect")
            .attr("class", 'perf_group_bg')
            .attr('x', 0)
            .attr('y', 0)
            .attr('height', metrics.singleIndexGroupHeight)
            .attr('width', metrics.extent(endTime.getTime() - startTime.getTime()))
            .on("click", (stat: Raven.Client.Data.Indexes.IndexingPerformanceBasicStats) => self.showDetails(stat));

        rect.datum(stats);
    }

    private findTimeRanges(): [Date, Date] {
        let minDateStr: string;
        let maxDateStr: string;

        this.data.forEach(indexStats => {
            indexStats.Performance.forEach(perfStat => {
                if (!minDateStr || perfStat.Started < minDateStr) {
                    minDateStr = perfStat.Started;
                }

                if (!maxDateStr || perfStat.Completed > maxDateStr) {
                    maxDateStr = perfStat.Completed;
                }
            });
        });

        let minDate: Date = minDateStr ? this.isoParser.parse(minDateStr) : null;
        let maxDate: Date = maxDateStr ? this.isoParser.parse(maxDateStr) : null;

        this.currentBatches.forEach(stats => {
            const statsStartedAsDate = this.isoParser.parse(stats.Started);
            if (!minDate || statsStartedAsDate.getTime() < minDate.getTime()) {
                minDate = statsStartedAsDate;
            }

            const statsEndDate = statsStartedAsDate.getTime() + stats.DurationInMilliseconds;
            const currentMaxDate = maxDate ? maxDate.getTime() : null;

            if (!currentMaxDate || statsEndDate > currentMaxDate) {
                maxDate = new Date(statsEndDate);
            }
        });

        return [minDate, maxDate];
    }

    private findIndexNames(): Array<string> {
        const names: Array<string> = [];

        this.data.forEach(x => {
            const indexName = x.IndexName;
            if (!names.contains(indexName)) {
                names.push(indexName);
            }
        });

        this.currentBatches.forEach((stats, indexName) => {
            if (!names.contains(indexName)) {
                names.push(indexName);
            }
        });
        
        return names;
    }

    private graphForIndex(stats: Raven.Client.Data.Indexes.IndexPerformanceStats, container: d3.Selection<Raven.Client.Data.Indexes.IndexPerformanceStats[]>) {
        stats.Performance.forEach(perf => {
            this.graphPerformanceGroup(container, stats.IndexName, perf);
        });
    }

    private static extent(timeExtent: number) {
        return timeExtent / 1000.0 * metrics.pixelsPerSecond;
    }

    private graphPerformanceGroup(container: d3.Selection<Raven.Client.Data.Indexes.IndexPerformanceStats[]>,
        indexName: string, data: Raven.Client.Data.Indexes.IndexingPerformanceStats) {
        const self = this;

        const startTime = self.isoParser.parse(data.Started);
        const endTime = self.isoParser.parse(data.Completed);

        const perfGroup = container.append("g")
            .attr('class', 'perf_group_item')
            .attr("transform", "translate(" + self.xScale(startTime) + "," + (self.yScale(indexName) + metrics.verticalPadding / 2) + ")");
        
        perfGroup.append("rect")
            .attr("class", 'perf_group_bg')
            .attr('x', 0)
            .attr('y', 0)
            .attr('height', metrics.singleIndexGroupHeight)
            .attr('width', metrics.extent(endTime.getTime() - startTime.getTime()));

        perfGroup.datum(data);

        self.drawOperations([data.Details], 4, perfGroup);
    }

    private drawOperations(ops: Raven.Client.Data.Indexes.IndexingPerformanceOperation[], level: number, parent: d3.Selection<any>) {
        const self = this;
        let xStart = 0;
        for (let i = 0; i < ops.length; i++) {
            const op = ops[i];

            const group = parent.append("g")
                .attr('class', 'op_group_level_' + level)
                .attr("transform", "translate(" + xStart +"," + metrics.innerGroupPadding + ")");

            const width = metrics.extent(op.DurationInMilliseconds);
            xStart += width;

            group
                .append("rect")
                .attr('x', 0)
                .attr('y', 0)
                .attr('height', metrics.singleIndexGroupHeight - (metrics.maxRecurseLevel - level + 1) * 2 * metrics.innerGroupPadding)
                .attr('width', width)
                .datum(op)
                .attr('fill', (data) => self.colorScale(data.Name))
                .on('click', (data) => self.showDetails(data));

            if (op.Operations.length > 0) {
                this.drawOperations(op.Operations, level - 1, group);
            }
        }
    }

    private showDetails(op: any) {
        var dialog = new tempStatDialog(op);
        app.showDialog(dialog);
    }

}

export = metrics; 
