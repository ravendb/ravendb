import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import database = require("models/database");
import appUrl = require("common/appUrl");
import getIndexingPerfStatsCommand = require("commands/getIndexingPerfStatsCommand");
import d3 = require("d3/d3");
import nv = require('nvd3');

class indexStats extends viewModelBase {
    jsonData: any[];

    margin = { top: 40, right: 20, bottom: 40, left: 40 };
    width: number;
    height: number;
    barPaddingFromTick = 5;
    isoFormat = d3.time.format.iso;
    xTickFormat = d3.time.format("%Y-%m-%d %H:%M:%S");	
    x0Scale: D3.Scale.OrdinalScale;
    x1Scale: D3.Scale.OrdinalScale;
    yScale: D3.Scale.LinearScale;
    color = d3.scale.category20();
    xAxis: D3.Svg.Axis;
    yAxis: D3.Svg.Axis;
    svg: D3.Selection;
    legend: D3.UpdateSelection;

    canActivate(args) {
        super.canActivate(args);

        var deferred = $.Deferred();

        $.when(this.fetchJsonData())
            .done((data) => {
                this.jsonData = data;
                deferred.resolve({ can: true });
                })
            .fail(() => deferred.resolve({ can: false }));

        return deferred;
    }

    fetchJsonData() {
        return new getIndexingPerfStatsCommand(this.activeDatabase()).execute();
    }

    attached() {
        $("#indexStatsContainer").resize().on('DynamicHeightSet', () => this.onWindowHeightChanged());
    }

    redrawGraph() {
        var self = this;
        var indexNames = this.findIndexNames(this.jsonData);
        this.width = $("#indexStatsContainer").width() - this.margin.left - this.margin.right;
        this.height = $("#indexStatsContainer").height() - this.margin.top - this.margin.bottom;
        this.x0Scale = d3.scale.ordinal().rangeRoundBands([0, self.width], 0.1);
        this.x1Scale = d3.scale.ordinal();
        this.yScale = d3.scale.linear().range([self.height, 0]);
        this.xAxis = d3.svg.axis()
            .scale(self.x0Scale)
            .orient("bottom")
            .tickFormat(d => self.xTickFormat(self.isoFormat.parse(d)))
            .tickPadding(20);
        this.yAxis = d3.svg.axis()
            .scale(self.yScale)
            .orient("left")
            .tickFormat(d3.format(".2s"));

        this.svg = d3.select("#indexStatsGraph")
            .attr("width", self.width + self.margin.left + self.margin.right)
            .attr("height", self.height + self.margin.top + self.margin.bottom);

        this.svg.selectAll('.main_group')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        var svgEnter = this.svg
            .selectAll(".main_group")
            .data([this.jsonData]).enter()
            .append('g')
            .attr('class', 'main_group')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        svgEnter.append("g")
            .attr("class", "x axis");

        svgEnter.append('g')
            .attr('class', 'y axis')

        svgEnter.select('.y.axis')
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", 6)
            .attr("dy", ".71em")
            .style("text-anchor", "end")
            .text("Doc count");

        this.x0Scale.domain(d3.nest()
            .key(d => d.Started)
            .sortKeys(d3.ascending)
            .entries(self.jsonData)
            .map(d => d.key));
        this.x1Scale.domain(indexNames).rangeRoundBands([0, self.x0Scale.rangeBand()]);

        this.yScale.domain([0, d3.max(this.jsonData,
            d => d3.max(<any[]>d.Stats,
                dd => d3.max([dd.InputCount, dd.OutputCount]))
            )]);

        this.svg.select(".x.axis")
            .attr("transform", "translate(0," + self.height + ")")
            .transition()
            .call(self.xAxis);

        this.svg.select('.y.axis')
            .transition()
            .call(self.yAxis);

        var frame = this.svg.select('.main_group').selectAll(".frame")
            .data(self.jsonData, d => d.Started);

        frame
            .transition()
            .attr("transform", d => "translate(" + self.x0Scale(d.Started) + ",0)");

        frame
            .select('.input_text')
            .transition()
            .attr('x', self.x0Scale.rangeBand() / 4)
            .attr('y', self.height + 20);

        frame
            .select('.output_text')
            .transition()
            .attr('x', self.x0Scale.rangeBand() * 3 / 4)
            .attr('y', self.height + 20);

        var frameEnter = frame.enter()
            .append("g")
            .attr("class", "frame")
            .attr("transform", d => "translate(" + self.x0Scale(d.Started) + ",0)");

        frameEnter.append("text")
            .attr('class', 'input_text')
            .attr("x", self.x0Scale.rangeBand() / 4)
            .attr("y", self.height + 20)
            .text("Input");

        frameEnter.append("text")
            .attr('class', 'output_text')
            .attr("x", self.x0Scale.rangeBand() * 3 / 4)
            .attr("y", self.height + 20)
            .text("Output");


        var inputCounts = frame.selectAll(".inputCounts")
            .data(d => d.Stats);

        inputCounts
            .transition()
            .attr("width", self.x1Scale.rangeBand() / 2)
            .attr("x", d => self.x1Scale(d.Index) / 2 - self.barPaddingFromTick)
            .attr("y", d => self.yScale(d.InputCount))
            .attr("height", d => self.height - self.yScale(d.InputCount));

        inputCounts.enter().append("rect")
            .attr("class", "inputCounts")
            .attr("width", self.x1Scale.rangeBand() / 2)
            .attr("x", d => self.x1Scale(d.Index) / 2 - self.barPaddingFromTick)
            .attr("y", d => self.yScale(d.InputCount))
            .attr("height", d => self.height - self.yScale(d.InputCount))
            .style("fill", d => self.color(d.Index))
            .on('click', function (d) {
                nv.tooltip.cleanup();
                var offset = $(this).offset();
                var containerOffset = $("#indexStatsContainer").offset();
                nv.tooltip.show([offset.left - containerOffset.left + self.x1Scale.rangeBand() / 2, offset.top - containerOffset.top], self.getTooltip(d), 's', 25, document.getElementById("indexStatsContainer"), "selectable-tooltip");
            });

        var outputCounts = frame.selectAll(".outputCounts")
            .data(d => d.Stats);

        outputCounts
            .transition()
            .attr("width", self.x1Scale.rangeBand() / 2)
            .attr("x", d => self.x1Scale(d.Index) / 2 + self.x0Scale.rangeBand() / 2 + self.barPaddingFromTick)
            .attr("y", d => self.yScale(d.OutputCount))
            .attr("height", d => self.height - self.yScale(d.OutputCount));

        outputCounts.enter().append("rect")
            .attr("class", "outputCounts")
            .attr("width", self.x1Scale.rangeBand() / 2)
            .attr("x", d => self.x1Scale(d.Index) / 2 + self.x0Scale.rangeBand() / 2 + self.barPaddingFromTick)
            .attr("y", d => self.yScale(d.OutputCount))
            .attr("height", d => self.height - self.yScale(d.OutputCount))
            .style("fill", d => self.color(d.Index))
            .on('click', function (d) {
                nv.tooltip.cleanup();
                var offset = $(this).offset();
                var containerOffset = $("#indexStatsContainer").offset();
                nv.tooltip.show([offset.left - containerOffset.left + self.x1Scale.rangeBand()  / 2, offset.top - containerOffset.top], self.getTooltip(d), 's', 25, document.getElementById("indexStatsContainer"), "selectable-tooltip");
            });

        this.legend = this.svg.select('.main_group').selectAll(".legend")
            .data(indexNames);

        this.legend.selectAll("rect").transition()
            .attr("x", this.width - 18);

        this.legend.selectAll("text").transition()
            .attr("x", this.width - 24)
            .text(d => d);

        var legendEnter = this.legend
            .enter().append("g")
            .attr("class", "legend")
            .attr("transform", function (d, i) { return "translate(0," + i * 20 + ")"; });

        legendEnter.append("rect")
            .attr("x", this.width - 18)
            .attr("width", 18)
            .attr("height", 18)
            .style("fill", self.color);

        legendEnter.append("text")
            .attr("x", this.width - 24)
            .attr("y", 9)
            .attr("dy", ".35em")
            .style("text-anchor", "end")
            .text(d => d);
    }

    onWindowHeightChanged() {
        nv.tooltip.cleanup();
        this.width = $("#indexStatsContainer").width();
        this.height = $("#indexStatsContainer").height();
        this.redrawGraph();
    }

    getTooltip(d) {
        return "<strong>Index:</strong> <span>" + d.Index + "</span><br />"
            + "<strong>Start time:</strong> <span>" + d.StartTime + "</span><br />"
            + "<strong>Duration milliseconds:</strong> <span>" + d.DurationMilliseconds + "</span><br />"
            + "<strong>Input count:</strong> <span>" + d.InputCount + "</span><br />"
            + "<strong>Output count:</strong> <span>" + d.OutputCount + "</span><br />"
            + "<strong>Items count:</strong> <span>" + d.ItemsCount + "</span><br />"
            ;
    }

    detached() {
        super.detached();

        $("#visualizerContainer").off('DynamicHeightSet');
        nv.tooltip.cleanup();
    }

    findIndexNames(jsonData) {
        var statsInline = d3.merge(jsonData.map((d) => d.Stats));
        var byKey = d3
            .nest()
            .key(d => d.Index)
            .sortKeys(d3.ascending)
            .rollup(l => l.length)
            .entries(statsInline);
        return byKey.map(d => d.key);
    }
}

export = indexStats;
