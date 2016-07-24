import viewModelBase = require("viewmodels/viewModelBase");
import generalUtils = require("common/generalUtils");
import getIndexingPerfStatsCommand = require("commands/database/debug/getIndexingPerfStatsCommand");
import nv = require('nvd3');
import changesContext = require("common/changesContext");
import changeSubscription = require('common/changeSubscription');

class indexStats extends viewModelBase {/*
    jsonData: any[] = [];
    rawJsonData: any[] = [];
    hiddenIndexes = d3.set([]);
    indexNames: string[] = [];
    private refreshGraphObservable = ko.observable<number>();
    private refreshSubscription: KnockoutSubscription;

    margin = { top: 40, right: 20, bottom: 40, left: 40 };
    barWidth = 30;
    width: number;
    height: number;
    barPaddingInner = 5;
    barPaddingOuter = 10;
    legendWidth = 0;
    isoFormat = d3.time.format.iso;
    xTickFormat = d3.time.format("%H:%M:%S");	
    x0Scale: D3.Scale.OrdinalScale;
    yScale: D3.Scale.LinearScale;
    color = d3.scale.category20();
    xAxis: D3.Svg.Axis;
    yAxis: D3.Svg.Axis;
    svg: D3.Selection;
    legend: D3.UpdateSelection;

    fetchJsonData() {
        return new getIndexingPerfStatsCommand(this.activeDatabase()).execute();
    }

    attached() {
        super.attached();
        $("#indexStatsContainer").resize().on('DynamicHeightSet', () => this.onWindowHeightChanged());
        $("#indexStatsContainer").scroll(() => this.graphScrolled());
        this.refreshSubscription = this.refreshGraphObservable.throttle(5000).subscribe((e) => this.refresh());
    }

    compositionComplete() {
        this.refresh();
    }

    createNotifications(): Array<changeSubscription> {
        return [changesContext.currentResourceChangesApi().watchAllIndexes(e => this.processIndexEvent(e)) ];
    }

    processIndexEvent(e: indexChangeNotificationDto) {
        if (e.Type == "MapCompleted" || e.Type == "ReduceCompleted") {
            this.refreshGraphObservable(new Date().getTime());
        }
    }

    filterJsonData() {
        this.jsonData = [];

        this.rawJsonData.forEach(v => {
            var filteredStats = v.Stats.filter(s => !this.hiddenIndexes.has(s.Index));
            if (filteredStats.length > 0) {
                this.jsonData.push({
                    'Started': v.Started,
                    'Stats': filteredStats
                });
            }
        });
    }

    refresh() {
        return this.fetchJsonData().done((data) => {
            this.rawJsonData = this.mergeJsonData(this.rawJsonData, data);
            this.indexNames = this.findIndexNames(this.rawJsonData);
            this.filterJsonData();
            this.redrawGraph(); 
        });
    }

    private mergeJsonData(currentData:any[], incomingData:any[]) {
        // create lookup map to avoid O(n^2) 
        var dateLookup = d3.map();
        currentData.forEach((d, i) => {
            dateLookup.set(d.Started, i);
        });

        incomingData.forEach(d => {
            if (dateLookup.has(d.Started)) {
                var index = dateLookup.get(d.Started);
                var exitingData = currentData[index];
                var newData = d;

                // we merge into existing data
                var indexLookup = d3.map();
                exitingData.Stats.forEach((d, i) => {
                    indexLookup.set(d.Index, i);
                });

                newData.Stats.forEach(s => {
                    if (indexLookup.has(s.Index)) {
                        var index = indexLookup.get(s.Index);
                        var dest = exitingData.Stats[index];
                        dest.DurationMilliseconds = Math.max(dest.DurationMilliseconds, s.DurationMilliseconds);
                        dest.InputCount = Math.max(dest.InputCount, s.InputCount);
                        dest.OutputCount = Math.max(dest.OutputCount, s.OutputCount);
                        dest.ItemsCount = Math.max(dest.ItemsCount, s.ItemsCount);
                    } else {
                        exitingData.Stats.push(s);
                    }
                });
            } else {
                currentData.push(d);
            }
        });
        return currentData;
    }

    private computeBarWidths(data: any[]) {
        var cumulative = 10;
        var result = data.map(perfData => {
            var prevValue = cumulative;
            perfData.sectionWidth = perfData.Stats.length * this.barWidth * 2 + this.barPaddingInner * 2 + this.barPaddingOuter;
            cumulative += perfData.sectionWidth;
            return prevValue;
        });
        result.push(cumulative);
        return result;
    }

    graphScrolled() {
        var leftScroll = $("#indexStatsContainer").scrollLeft();
        var self = this;
        this.svg.select('.y.axis')
            .attr("transform", "translate(" + leftScroll + ",0)");

        this.svg.select('#dataClip rect')
            .attr('x', leftScroll);

        this.svg.select('.legend_bg_group')
            .attr("transform", "translate(" + leftScroll + ",0)");

        this.svg.select('.controlls')
            .selectAll(".legend")
            .attr("transform", function (d, i) { return "translate(" + leftScroll + "," + i * 20 + ")"; });
        nv.tooltip.cleanup();
    }

    toggleIndexVisible(indexName) {
        nv.tooltip.cleanup();
        var alreadyHidden = this.hiddenIndexes.has(indexName);
        if (alreadyHidden) {
            this.hiddenIndexes.remove(indexName);
        } else {
            this.hiddenIndexes.add(indexName);
        }
        d3.select('.rect-legend-' + generalUtils.escape(indexName)).classed('legendHidden', !alreadyHidden);
        this.filterJsonData();
        this.redrawGraph();
        // we have to manually trigger on scroll even to fix firefox issue (missing event call)
        this.graphScrolled();
    }

    redrawGraph() {
        var self = this;
        
        this.width = $("#indexStatsContainer").width() - this.margin.left - this.margin.right;
        this.height = $("#indexStatsContainer").height() - this.margin.top - this.margin.bottom - 20; // substract scroll width
        
        var cumulativeWidths = this.computeBarWidths(this.jsonData);

        this.x0Scale = d3.scale.ordinal().range(cumulativeWidths);
        this.yScale = d3.scale.linear().range([self.height, 0]);
        this.xAxis = d3.svg.axis()
            .scale(self.x0Scale)
            .orient("bottom")
            .tickFormat(d => "")
            .tickPadding(20);
        this.yAxis = d3.svg.axis()
            .scale(self.yScale)
            .orient("left")
            .tickFormat(d3.format(".2s"));

        var totalHeight = self.height + self.margin.top + self.margin.bottom;

        // get higer value from total (visiable and not visible graph width) and viewbox width.
        var totalWidth = Math.max(cumulativeWidths[cumulativeWidths.length - 1], this.width) + this.margin.left + this.margin.right;

        $("#indexStatsContainer").css('overflow-x', cumulativeWidths[cumulativeWidths.length - 1] > this.width ? 'scroll' : 'hidden');

        this.svg = d3.select("#indexStatsGraph")
            .attr("width", totalWidth)
            .attr("height", totalHeight)
            .style({ height: totalHeight + 'px' })
            .style({ width: totalWidth + 'px' })
            .attr("viewBox", "0 0 " + totalWidth + " " + totalHeight);

        this.svg.selectAll('.main_group')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        this.svg
            .selectAll('defs')
            .data([this.jsonData])
            .enter()
            .append('defs')
            .append('clipPath')
            .attr('id', 'dataClip')
            .append('rect')
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 1200000)
            .attr('height', 50000);

        var svgEnter = this.svg
            .selectAll(".main_group")
            .data([this.jsonData]).enter();

        svgEnter.append('g')
            .attr('class', 'main_group')
            .attr('clip-path', "url(#dataClip)")
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        var controllsEnter = this.svg
            .selectAll(".controlls")
            .data([this.jsonData]).enter()
            .append("g")
            .attr('class', 'controlls')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        controllsEnter.append("g")
            .attr("class", "x axis");

        controllsEnter.append('g')
            .attr('class', 'y axis')

        controllsEnter.append('g')
            .attr('class', 'legend_bg_group')
            .append('rect')
            .attr('class', 'legend_bg')
            .attr('x', self.width)
            .attr('y', 0)
            .attr('width', 0)
            .attr('height', 0);

        controllsEnter.select('.y.axis')
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

        this.yScale.domain([0, d3.max(this.jsonData,
            d => d3.max(<any[]>d.Stats,
                dd => d3.max([dd.InputCount, dd.OutputCount]))
            )]);

        this.svg.select(".x.axis")
            .attr('clip-path', "url(#dataClip)")
            .attr("transform", "translate(0," + self.height + ")")
            .transition()
            .call(self.xAxis);

        this.svg.select('.y.axis')
            .transition()
            .call(self.yAxis);

        var frame = this.svg.select('.main_group').selectAll(".frame")
            .data(self.jsonData, d => d.Started);

        frame.exit().remove();

        frame
            .transition()
            .attr("transform", d => "translate(" + self.x0Scale(d.Started) + ",0)");

        frame
            .select('.date_tick')
            .transition()
            .attr('x', d => d.sectionWidth / 2)
            .attr('y', self.height + 36);

        frame
            .select('.input_text')
            .transition()
            .attr("x", d => d.sectionWidth * 0.25)
            .attr('y', self.height + 18);

        frame
            .select('.output_text')
            .transition()
            .attr("x", d => d.sectionWidth * 0.75)
            .attr('y', self.height + 18);

        frame
            .select('.outputs')
            .transition()
            .attr("transform", d => "translate(" + (d.sectionWidth / 2) + ",0)");

        var frameEnter = frame.enter()
            .append("g")
            .attr("class", "frame")
            .attr("transform", d => "translate(" + self.x0Scale(d.Started) + ",0)");

        frameEnter.append("text")
            .attr('class', 'input_text')
            .attr('text-anchor', 'middle')
            .attr("x", d => d.sectionWidth * 0.25)
            .attr("y", self.height + 18)
            .text("In");

        frameEnter.append("text")
            .attr('class', 'output_text')
            .attr('text-anchor', 'middle')
            .attr("x", d => d.sectionWidth * 0.75)
            .attr("y", self.height + 18)
            .text("Out");

        frameEnter.append("text")
            .attr('class', 'date_tick')
            .attr('text-anchor', 'middle')
            .attr('x', d => d.sectionWidth / 2)
            .attr('y', self.height + 36)
            .text(d => self.xTickFormat(self.isoFormat.parse(d.Started)));

        frameEnter.append("g")
            .attr('class', 'inputs');

        frameEnter.append('g')
            .attr('class', 'outputs')
            .attr("transform", d => "translate(" + (d.sectionWidth / 2) + ",0)");
            
        var inputCounts = frame.select('.inputs').selectAll(".inputCounts")
            .data(d => d.Stats, d => d.Index);

        inputCounts.exit().remove();

        inputCounts
            .transition()
            .attr("width", self.barWidth)
            .attr("x", (d, i) => i * self.barWidth - self.barPaddingInner + self.barPaddingOuter)
            .attr("y", d => self.yScale(d.InputCount))
            .attr("height", d => self.height - self.yScale(d.InputCount))
            .style("fill", d => self.color(d.Index));

        inputCounts.enter().append("rect")
            .attr("class", "inputCounts")
            .attr("width", self.barWidth)
            .attr("x", (d, i) => i * self.barWidth - self.barPaddingInner + self.barPaddingOuter)
            .attr("y", d => self.height)
            .attr("height", 0)
            .style("fill", d => self.color(d.Index))
            .on('click', function (d) {
                nv.tooltip.cleanup();
                var offset = $(this).offset();
                var leftScroll = $("#indexStatsContainer").scrollLeft();
                var containerOffset = $("#indexStatsContainer").offset();
                nv.tooltip.show([offset.left - containerOffset.left + leftScroll + self.barWidth, offset.top - containerOffset.top], self.getTooltip(d), 's', 5, document.getElementById("indexStatsContainer"), "selectable-tooltip");
                $(".nvtooltip").each((i, elem) => {
                    ko.applyBindings(self, elem);
                });
            })
            .transition()
            .attr("height", d => self.height - self.yScale(d.InputCount))
            .attr("y", d => self.yScale(d.InputCount));


        var outputCounts = frame.select('.outputs').selectAll(".outputCounts")
            .data(d => d.Stats, d => d.Index);

        outputCounts.exit().remove();

        outputCounts
            .transition()
            .attr("width", self.barWidth)
            .attr("x", (d, i) => i * self.barWidth + self.barPaddingInner)
            .attr("y", d => self.yScale(d.OutputCount))
            .attr("height", d => self.height - self.yScale(d.OutputCount))
            .style("fill", d => self.color(d.Index));

        outputCounts.enter().append("rect")
            .attr("class", "outputCounts")
            .attr("width", self.barWidth)
            .attr("x", (d, i) => i * self.barWidth + self.barPaddingInner)
            .attr("y", d => self.height)
            .attr("height", 0)
            .style("fill", d => self.color(d.Index))
            .on('click', function (d) {
                nv.tooltip.cleanup();
                var offset = $(this).offset();
                var leftScroll = $("#indexStatsContainer").scrollLeft();
                var containerOffset = $("#indexStatsContainer").offset();
                nv.tooltip.show([offset.left - containerOffset.left + leftScroll + self.barWidth, offset.top - containerOffset.top], self.getTooltip(d), 's', 5, document.getElementById("indexStatsContainer"), "selectable-tooltip");
                $(".nvtooltip").each((i, elem) => {
                    ko.applyBindings(self, elem);
                });
            })
            .transition()
            .attr("y", d => self.yScale(d.OutputCount))
            .attr("height", d => self.height - self.yScale(d.OutputCount));

        this.legend = this.svg.select('.controlls').selectAll(".legend")
            .data(this.indexNames, d => d);

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
            .attr('class', d => 'rect-legend-' + generalUtils.escape(d))
            .attr("width", 18)
            .attr("height", 18)
            .style("fill", self.color)
            .style("stroke", self.color)
            .on('click', d => self.toggleIndexVisible(d));

        legendEnter.append("text")
            .attr("x", this.width - 24)
            .attr("y", 9)
            .attr("dy", ".35em")
            .style("text-anchor", "end")
            .text(d => d);

        // Bug fix: default to zero if we don't have any .legend text objects. This can happen when 
        // getIndexPerfStatsCommand returns an empty array. See http://issues.hibernatingrhinos.com/issue/RavenDB-2929
        this.legendWidth = (d3.max(<any>$(".legend text"), (d: any) => d.getBBox().width) + 40)
            || 0;

        this.svg.select('.legend_bg')
            .attr('y', -6)
            .attr('height', this.indexNames.length * 20 + 10)
            .attr('width', this.legendWidth)
            .attr('x', this.width - this.legendWidth + 10);
    }

    onWindowHeightChanged() {
        nv.tooltip.cleanup();
        this.width = $("#indexStatsContainer").width();
        this.height = $("#indexStatsContainer").height();
        this.redrawGraph();
    }

    getTooltip(d) {
        return '<button type="button" class="close" data-bind="click: tooltipClose" aria-hidden="true"><i class="fa fa-times"></i></button>'
            + "<strong>Index:</strong> <span>" + d.Index + "</span><br />"
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
        if (this.refreshSubscription != null) {
            this.refreshSubscription.dispose();
        }
    }

    tooltipClose() {
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
    }*/
}

export = indexStats;
