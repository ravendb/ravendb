import viewModelBase = require("viewmodels/viewModelBase");
import app = require("durandal/app");
import tempStatDialog = require("viewmodels/database/status/indexing/tempStatDialog");
import getIOMetricsCommand = require("commands/database/debug/getIOMetricsCommand");
import fileDownloader = require("common/fileDownloader");
import d3 = require("d3");

class ioStats extends viewModelBase { 

    private data: Raven.Server.Documents.Handlers.IOMetricsResponse;

    private static readonly barHeight = 16;
    private static readonly pixelsPerSecond = 8;
    private static readonly betweenGroupPadding = 10;
    private static readonly leftPadding = 20;
    private static readonly topPadding = 30;
    private static readonly legendWidth = 500;

    private isoParser = d3.time.format.iso;
    private svg: d3.Selection<Raven.Server.Documents.Handlers.IOMetricsResponse>;

    private xScale: d3.time.Scale<number, number>;
    private xAxis: d3.svg.Axis;

    private yScale: d3.scale.Ordinal<string, number>;
    private yAxis: d3.svg.Axis;

    private xTickFormat = d3.time.format("%H:%M:%S");

    private graphData: d3.Selection<Raven.Server.Documents.Handlers.IOMetricsResponse>;

    private commonPathsPrefix: string;

    activate(args: any) {
        super.activate(args);

        return new getIOMetricsCommand(this.activeDatabase())
            .execute()
            .done(result => this.data = result);
    }

    attached() {
        super.attached();
        this.svg = d3.select("#ioStatsGraph");
    }

    compositionComplete() {
        super.compositionComplete();

        this.draw();
    }

    private draw() {
        if (!this.data || this.data.Environments.length === 0) {
            return;
        }

        this.ensurePathsEndsWithSlash();

        this.commonPathsPrefix = this.findCommonPathPrefix();

        const [minTime, maxTime] = this.findTimeRanges();

        const files = this.findFileNames();

        this.drawContainer(minTime, maxTime, files);

        this.fillGraph();
    }

    private ensurePathsEndsWithSlash() {
        this.data.Environments.forEach(env => {
            if (!env.Path.endsWith("\\")) {
                env.Path += "\\";
            }
        });
    }

    private findFileNames(): Array<string> {
        const result = [] as Array<string>;
        this.data.Environments.forEach(env => {
            env.Files.forEach(file => {
                result.push(this.normalizeFileName(env.Path, file.File));
            });
        });
        return result;
    }

    private normalizeFileName(path: string, fileName: string) {
        if (fileName.endsWith(".journal")) {
            fileName = "*.journal";
        }

        return path.substring(this.commonPathsPrefix.length) + fileName;
    }

    private findCommonPathPrefix() {
        const paths = this.data.Environments.map(env => env.Path);
        return this.findPrefix(paths);
    }

    private findPrefix(strings: Array<string>) {
        if (!strings.length) {
            return "";
        }

        const sorted = strings.slice(0).sort();
        const string1 = sorted[0];
        const string2 = sorted[sorted.length - 1];
        let i = 0;
        const l = Math.min(string1.length, string2.length);

        while (i < l && string1[i] === string2[i]) {
            i++;
        }

        return string1.slice(0, i);
    }

    private findTimeRanges(): [Date, Date] {
        let minDate: string = null;
        let maxDateAsNumber: number = null;

        this.data.Environments.forEach(env => {
            env.Files.forEach(file => {
                file.History.forEach(historyItem => {
                    if (!minDate || minDate > historyItem.Start) {
                        minDate = historyItem.Start;
                    }
                    const itemEndDate = this.isoParser.parse(historyItem.End).getTime();
                    if (!maxDateAsNumber || maxDateAsNumber < itemEndDate) {
                        maxDateAsNumber = itemEndDate;
                    }
                });
                file.Recent.forEach(recentItem => {
                    if (!minDate || minDate > recentItem.Start) {
                        minDate = recentItem.Start;
                    }

                    const itemEndDate = this.isoParser.parse(recentItem.Start).getTime() + recentItem.Duration;
                    if (!maxDateAsNumber || maxDateAsNumber < itemEndDate) {
                        maxDateAsNumber = itemEndDate;
                    }
                });
            });
        });

        return [minDate ? this.isoParser.parse(minDate) : null, new Date(maxDateAsNumber)];
    }

    private static extent(timeExtent: number) {
        return timeExtent / 1000.0 * ioStats.pixelsPerSecond;
    }

    private drawContainer(minTime: Date, maxTime: Date, files: Array<string>) {
        const timeExtent = maxTime.getTime() - minTime.getTime();
        const totalWidth = ioStats.extent(timeExtent);

        this.xScale = d3.time.scale<number>()
            .range([0, totalWidth])
            .domain([minTime, maxTime]);

        this.svg.append("g")
            .attr("class", "x axis");

        const ticks = d3.scale.linear()
            .domain([0, timeExtent])
            .ticks(Math.ceil(timeExtent / 10000)).map(y => this.xScale.invert(y));

        this.xAxis = d3.svg.axis()
            .scale(this.xScale)
            .orient("top")
            .tickValues(ticks)
            .tickSize(10)
            .tickFormat(this.xTickFormat);

        const totalHeight = files.length * (ioStats.barHeight + ioStats.betweenGroupPadding);

        this.svg.select(".x.axis")
            .attr("transform", "translate(" + (ioStats.leftPadding + ioStats.legendWidth) + "," + ioStats.topPadding + ")")
            .call(this.xAxis);

        this.yScale = d3.scale.ordinal()
            .domain(files)
            .rangeBands([0, totalHeight]);

        this.yAxis = d3.svg.axis()
            .scale(this.yScale)
            .orient("left");

        this.svg.append('g')
            .attr('class', 'y axis map')
            .attr("transform", "translate(" + (ioStats.leftPadding + ioStats.legendWidth) + "," + ioStats.topPadding + ")");

        this.svg.select(".y.axis.map")
            .call(this.yAxis);

        $("#ioStatsGraph")
            .width(ioStats.leftPadding + ioStats.legendWidth + totalWidth + 20)
            .height(totalHeight + ioStats.topPadding + 20);

        this.graphData = this.svg.append("g")
            .attr('class', 'graph_data')
            .attr('transform', "translate(" + (ioStats.leftPadding + ioStats.legendWidth) + "," + (ioStats.topPadding + ioStats.betweenGroupPadding / 2) + ")");
    }

    private fillGraph() {
        this.data.Environments.forEach(env => {
            const path = env.Path;
            env.Files.forEach(fileStats => {
                const file = fileStats.File;
                const normalizedFileName = this.normalizeFileName(path, file);

                const group = this.graphData.append('g')
                    .attr('class', 'per_file_group')
                    .attr('data-filename', normalizedFileName)
                    .attr('transform', `translate(0,${ this.yScale(normalizedFileName) })`);

                fileStats.Recent.forEach(recentItem => {
                    (recentItem as any).file = file; // add information about file, since we join journals into single line

                    group.append('rect')
                        .attr('class', 'type_' + recentItem.Type)
                        .attr('x', this.xScale(this.isoParser.parse(recentItem.Start)))
                        .attr('y', ioStats.calcOffset(recentItem.Type))
                        .attr('width', Math.max(1, ioStats.extent(recentItem.Duration)))
                        .attr('height', ioStats.calcBarHeight(recentItem.Type))
                        .datum(recentItem)
                        .on('click', (data) => this.showDetails(data));
                });

                fileStats.History.forEach(history => {
                    (history as any).file = file; // add information about file, since we join journals into single line

                    group.append('rect')
                        .attr('class', 'type_bg_' + history.Type)
                        .attr('x', this.xScale(this.isoParser.parse(history.Start)))
                        .attr('y', ioStats.calcOffset(history.Type))
                        .attr('width', Math.max(1, ioStats.extent(history.Duration)))
                        .attr('height', ioStats.calcBarHeight(history.Type));

                    group.append('rect')
                        .attr('class', 'type_fg_' + history.Type)
                        .attr('x', this.xScale(this.isoParser.parse(history.Start)))
                        .attr('y', ioStats.calcOffset(history.Type))
                        .attr('width', Math.max(1, ioStats.extent(history.ActiveDuration)))
                        .attr('height', ioStats.calcBarHeight(history.Type))
                        .datum(history)
                        .on('click', (data) => this.showDetails(data));
                        
                });
            });
        });
    }

    private showDetails(op: any) {
        const dialog = new tempStatDialog(op);
        app.showBootstrapDialog(dialog);
    }

    static calcBarHeight(type: Sparrow.MeterType) {
        return type === "JournalWrite" ? ioStats.barHeight : ioStats.barHeight / 2;
    }

    static calcOffset(type: Sparrow.MeterType) {
        return type === "DataSync" ? ioStats.barHeight / 2 : 0;
    }


    exportAsJson() {
        fileDownloader.downloadAsJson({
            data: this.data
        }, "ioStats.json", "perf");
    }

    fileSelected() {
        const fileInput = <HTMLInputElement>document.querySelector("#importFilePicker");
        const self = this;
        if (fileInput.files.length === 0) {
            return;
        }

        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = function () {
            // ReSharper disable once SuspiciousThisUsage
            self.dataImported(this.result);
        };
        reader.onerror = function (error: any) {
            alert(error);
        };
        reader.readAsText(file);
    }

    private dataImported(result: string) {
        const json = JSON.parse(result) as {
            data: Raven.Server.Documents.Handlers.IOMetricsResponse;
        };

        this.data = json.data;

        $("#ioStatsGraph").empty();
        this.draw();
    }

}

export = ioStats; 
 
