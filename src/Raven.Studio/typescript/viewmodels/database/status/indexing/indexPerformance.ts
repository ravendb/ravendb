import viewModelBase = require("viewmodels/viewModelBase");
import app = require("durandal/app");
import tempStatDialog = require("viewmodels/database/status/indexing/tempStatDialog");
import getIndexesPerformance = require("commands/database/debug/getIndexesPerformance");

class metrics extends viewModelBase { 

    data: Raven.Client.Data.Indexes.IndexPerformanceStats[] = [];

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

    activate(args: any): JQueryPromise<Raven.Client.Data.Indexes.IndexPerformanceStats[]> {
        super.activate(args);
        return new getIndexesPerformance(this.activeDatabase())
            .execute()
            .done(result => this.data = result);
    }

    attached() {
        super.attached();
        this.svg = d3.select("#indexPerformanceGraph");
    }

    compositionComplete() {
        super.compositionComplete();

        this.draw();
    }

    private draw() {
        const self = this;
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
    }

    private findTimeRanges(): [Date, Date] {
        let minDate: string;
        let maxDate: string;

        this.data.forEach(indexStats => {
            indexStats.Performance.forEach(perfStat => {
                if (!minDate || perfStat.Started < minDate) {
                    minDate = perfStat.Started;
                }

                if (!maxDate || perfStat.Completed > maxDate) {
                    maxDate = perfStat.Completed;
                }
            });
        });

        return [this.isoParser.parse(minDate), this.isoParser.parse(maxDate)];
    }

    private findIndexNames(): Array<string> {
        const names: Array<string> = [];

        this.data.forEach(x => {
            const indexName = x.IndexName;
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

    private showDetails(op: Raven.Client.Data.Indexes.IndexingPerformanceOperation) {
        var dialog = new tempStatDialog(op);
        app.showDialog(dialog);
    }

}

export = metrics; 
