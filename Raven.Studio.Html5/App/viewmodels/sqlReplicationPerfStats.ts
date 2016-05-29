import viewModelBase = require("viewmodels/viewModelBase");
import generalUtils = require("common/generalUtils");
import changesContext = require("common/changesContext");
import getSqlReplicationPerfStatsCommand = require("commands/getSqlReplicationPerfStatsCommand");
import d3 = require("d3/d3");
import nv = require('nvd3');
import database = require('models/database');
import changeSubscription = require('models/changeSubscription');

class sqlReplicationPerfStats extends viewModelBase {
    statsAvailable: KnockoutComputed<boolean>;
    hasReplicationEnabled = ko.observable(false);

    jsonData: any[] = [];
    rawJsonData: any[] = [];
    hiddenNames = d3.set([]);
    replicationNames = ko.observableArray<string>([]);
    private refreshGraphObservable = ko.observable<number>();
    private refreshSubscription: KnockoutSubscription;

    margin = { top: 40, right: 20, bottom: 40, left: 40 };
    barWidth = 30;
    width: number;
    height: number;
    barPadding = 15;
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
        return new getSqlReplicationPerfStatsCommand(this.activeDatabase()).execute();
    }

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe((db: database) => {
            this.checkIfHasReplicationEnabled(db);
        });
        this.checkIfHasReplicationEnabled(this.activeDatabase());

        this.statsAvailable = ko.computed(() => this.hasReplicationEnabled() && this.replicationNames().length > 0);
    }

    checkIfHasReplicationEnabled(db: database) {
        this.hasReplicationEnabled(db.isBundleActive("sqlreplication"));
    }

    attached() {
        super.attached();
        $("#replicationStatsContainer").resize().on('DynamicHeightSet', () => this.onWindowHeightChanged());
        $("#replicationStatsContainer").scroll(() => this.graphScrolled());
        this.refresh();
        this.refreshSubscription = this.refreshGraphObservable.throttle(5000).subscribe((e) => this.refresh());
    }

    createNotifications(): Array<changeSubscription> {
        return [changesContext.currentResourceChangesApi().watchDocsStartingWith("Raven/SqlReplication/Status", e => this.processUpdate(e)) ];
    }

    processUpdate(e: documentChangeNotificationDto) {
        this.refreshGraphObservable(new Date().getTime());
    }

    filterJsonData() {
        this.jsonData = [];

        this.rawJsonData.forEach(v => {
            var filteredStats = v.Stats.filter(s => !this.hiddenNames.has(s.ReplicationName));
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
            this.replicationNames(this.findReplicationNames(this.rawJsonData));
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
                currentData[index] = d;
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
            perfData.sectionWidth = perfData.Stats.length * this.barWidth + this.barPadding * 2;
            cumulative += perfData.sectionWidth;
            return prevValue;
        });
        result.push(cumulative);
        return result;
    }

    graphScrolled() {
        var leftScroll = $("#replicationStatsContainer").scrollLeft();
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

    toggleGroupVisible(groupName) {
        nv.tooltip.cleanup();
        var alreadyHidden = this.hiddenNames.has(groupName);
        if (alreadyHidden) {
            this.hiddenNames.remove(groupName);
        } else {
            this.hiddenNames.add(groupName);
        }
        d3.select('.rect-legend-' + generalUtils.escape(groupName)).classed('legendHidden', !alreadyHidden);
        this.filterJsonData();
        this.redrawGraph();
        // we have to manually trigger on scroll even to fix firefox issue (missing event call)
        this.graphScrolled();
    }

    redrawGraph() {
        var self = this;
        
        this.width = $("#replicationStatsContainer").width() - this.margin.left - this.margin.right;
        this.height = $("#replicationStatsContainer").height() - this.margin.top - this.margin.bottom - 20; // substract scroll width
        
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

        $("#replicationStatsContainer").css('overflow-x', cumulativeWidths[cumulativeWidths.length - 1] > this.width ? 'scroll' : 'hidden');

        this.svg = d3.select("#replicationStatsGraph")
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
            .text("Batch size");

        this.x0Scale.domain(d3.nest()
            .key(d => d.Started)
            .sortKeys(d3.ascending)
            .entries(self.jsonData)
            .map(d => d.key));

        this.yScale.domain([0, d3.max(this.jsonData, d => d3.max(<any[]>d.Stats, dd => dd.BatchSize))]);

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
            .attr('y', self.height + 16);

        var frameEnter = frame.enter()
            .append("g")
            .attr("class", "frame")
            .attr("transform", d => "translate(" + self.x0Scale(d.Started) + ",0)");

        frameEnter.append("text")
            .attr('class', 'date_tick')
            .attr('text-anchor', 'middle')
            .attr('x', d => d.sectionWidth / 2)
            .attr('y', self.height + 16)
            .text(d => self.xTickFormat(self.isoFormat.parse(d.Started)));

        frameEnter.append("g")
            .attr('class', 'inputs');

        var inputCounts = frame.select('.inputs').selectAll(".inputCounts")
            .data(d => d.Stats, d => d.ReplicationName);

        inputCounts.exit().remove();

        inputCounts
            .transition()
            .attr("width", self.barWidth)
            .attr("x", (d, i) => i * self.barWidth + self.barPadding)
            .attr("y", d => self.yScale(d.BatchSize))
            .attr("height", d => self.height - self.yScale(d.BatchSize))
            .style("fill", d => self.color(d.ReplicationName));

        inputCounts.enter().append("rect")
            .attr("class", "inputCounts")
            .attr("width", self.barWidth)
            .attr("x", (d, i) => i * self.barWidth + self.barPadding)
            .attr("y", d => self.height)
            .attr("height", 0)
            .style("fill", d => self.color(d.ReplicationName))
            .on('click', function (d) {
                nv.tooltip.cleanup();
                var offset = $(this).offset();
                var leftScroll = $("#replicationStatsContainer").scrollLeft();
                var containerOffset = $("#replicationStatsContainer").offset();
                nv.tooltip.show([offset.left - containerOffset.left + leftScroll + self.barWidth, offset.top - containerOffset.top], self.getTooltip(d), 's', 5, document.getElementById("replicationStatsContainer"), "selectable-tooltip");
            })
            .transition()
            .attr("height", d => self.height - self.yScale(d.BatchSize))
            .attr("y", d => self.yScale(d.BatchSize));

        
        this.legend = this.svg.select('.controlls').selectAll(".legend")
            .data(this.replicationNames(), d => d);

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
            .on('click', d => self.toggleGroupVisible(d));

        legendEnter.append("text")
            .attr("x", this.width - 24)
            .attr("y", 9)
            .attr("dy", ".35em")
            .style("text-anchor", "end")
            .text(d => d);

        this.legendWidth = d3.max(<any>$(".legend text"), (d: any) => d.getBBox().width) + 40 || 0;

        this.svg.select('.legend_bg')
            .attr('y', -6)
            .attr('height', this.replicationNames().length * 20 + 10)
            .attr('width', this.legendWidth)
            .attr('x', this.width - this.legendWidth + 10);
    }

    onWindowHeightChanged() {
        nv.tooltip.cleanup();
        this.width = $("#replicationStatsContainer").width();
        this.height = $("#replicationStatsContainer").height();
        this.redrawGraph();
    }

    getTooltip(d) {
        return "<strong>Replication Name:</strong> <span>" + d.ReplicationName + "</span><br />"
            + "<strong>Duration milliseconds:</strong> <span>" + d.DurationMilliseconds + "</span><br />"
            + "<strong>Batch size:</strong> <span>" + d.BatchSize + "</span><br />"
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

    findReplicationNames(jsonData) {
        var statsInline = d3.merge(jsonData.map((d) => d.Stats));
        var byKey = d3
            .nest()
            .key(d => d.ReplicationName)
            .sortKeys(d3.ascending)
            .rollup(l => l.length)
            .entries(statsInline);
        return byKey.map(d => d.key);
    }

}

export = sqlReplicationPerfStats;
