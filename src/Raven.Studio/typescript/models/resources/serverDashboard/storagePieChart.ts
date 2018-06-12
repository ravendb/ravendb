/// <reference path="../../../../typings/tsd.d.ts"/>

import d3 = require('d3');

class storagePieChart {

    private width: number;
    private svg: d3.Selection<void>;
    private colorScale: d3.scale.Ordinal<string, string>;
    private containerSelector: string;
    
    constructor(containerSelector: string) {
        this.containerSelector = containerSelector;
        const container = d3.select(containerSelector);

        const $container = $(containerSelector);

        this.width = Math.min($container.innerHeight(), $container.innerWidth());

        this.svg = container
            .append("svg")
            .attr("width", this.width)
            .attr("height", this.width);
        
        this.svg
            .append("g")
            .attr("class", "pie")
            .attr("transform", "translate(" + (this.width / 2) + ", " + (this.width / 2) + ")");
        
        this.colorScale = d3.scale.ordinal<string>()
            .range(["#046293", "#fed101", "#689f39", "#ac2258", "#66418c", "#313fa0", "#ec407a", "#ff7000", "#890e4f", "#a487ba"]);
    }
    
    onResize() {
        const container = d3.select(this.containerSelector);

        const $container = $(this.containerSelector);

        this.width = Math.min($container.innerHeight(), $container.innerWidth());

        this.svg = container
            .select("svg")
            .attr("width", this.width)
            .attr("height", this.width);
        
        this.svg
            .select("g.pie")
            .attr("transform", "translate(" + (this.width / 2) + ", " + (this.width / 2) + ")")
            .selectAll(".arc")
            .remove();
    }
    
    getColorProvider() {
        return (dbName: string) => this.colorScale(dbName);
    }

    onData(data: Array<{ Database: string, Size: number }>, withTween = false) {
        const group = this.svg.select(".pie");

        const arc = d3.svg.arc<Raven.Server.Dashboard.DatabaseDiskUsage>()
            .innerRadius(16)
            .outerRadius(this.width / 2 - 10);

        const pie = d3.layout.pie<{ Database: string, Size: number }>()
            .value(x => x.Size)
            .sort(null);

        const arcs = group.selectAll(".arc")
            .data(pie(data));
        
        arcs.exit().remove();
        
        const enteringArcs = arcs
            .enter()
            .append("g")
            .attr("class", "arc");
       
        enteringArcs.append("path")
            .attr("d", d => arc(d as any))
            .attr("fill", d => this.colorScale(d.data.Database))
            .each(function (d) { this._current = d; });

        if (withTween) {
            const arcTween = function (a: d3.layout.pie.Arc<{ Database: string, Size: number }>) {
                const i = d3.interpolate(this._current , a as any);
                this._current = i(0);
                return (t: number) => arc(i(t) as any);
            };
            
            arcs.select("path")
                .transition()
                .attrTween("d", arcTween)
                .attr("fill", d => this.colorScale(d.data.Database));
        } else {
            arcs.select("path")
                .attr("d", d => arc(d as any))
                .attr("fill", d => this.colorScale(d.data.Database));
        }

    }
   
}

export = storagePieChart;
