/// <reference path="../../typings/tsd.d.ts"/>

import d3 = require("d3");

interface graphNode extends d3.layout.partition.Node {
    name: string;
    duration: number;
    visible: boolean;
} 

class timingsChart {

    // used for solve issue: https://github.com/d3/d3-hierarchy/issues/50
    static readonly fakeFill = "FAKE FILL";
    
    useLogScale = ko.observable<boolean>(false);

    private totalSize = 0;
    private data: Raven.Client.Documents.Queries.Timings.QueryTimings;
    rootNode = ko.observable<graphNode>();
    
    vis: d3.Selection<any>;
    levelName: d3.Selection<any>;
    levelDuration: d3.Selection<any>;
    
    constructor(private selector: string) {
        this.useLogScale.subscribe(() => this.draw(this.data));
    }
    
    draw(data: Raven.Client.Documents.Queries.Timings.QueryTimings) {
        this.data = data;
        d3.select(this.selector).select("svg").remove();
        
        const container = $(this.selector);
        
        const topPadding = 50;
        
        const width = container.width();
        const height = container.height();
        const radius = Math.min(width, height) - topPadding;
        
        const legendWidth = 270;
        
        this.vis = d3.select(this.selector)
            .append("svg:svg")
            .attr("width", width)
            .attr("height", height)
            .append("svg:g")
            .attr("id", "container")
            .attr("transform", "translate(" + (width / 2 + legendWidth / 2) + "," + (height - 20) + ")");
        
        const useLogScale = this.useLogScale();
        
        const partition = d3.layout.partition<graphNode>()
            .sort(null)
            .size([Math.PI, radius * radius])
            .value(x => (useLogScale ? Math.log1p(x.duration) : x.duration) || 0.01);
        
        const arc = d3.svg.arc<graphNode>()
            .startAngle(d => -0.5 * Math.PI + d.x)
            .endAngle(d => -0.5 * Math.PI + d.x + d.dx)
            .innerRadius(d => Math.sqrt(d.y))
            .outerRadius(d => Math.sqrt(d.y + d.dy));
        
        const json = this.convertHierarchy("Total", data);
        this.rootNode(json);

        this.totalSize = data.DurationInMs;
        
        // Bounding circle underneath the sunburst, to make it easier to detect
        // when the mouse leaves the parent g.
        this.vis
            .append("svg:circle")
            .attr("r", radius)
            .style("opacity", 0);

        const nodes = partition
            .nodes(json)
            .filter(x => x.name !== timingsChart.fakeFill);

        this.levelName = this.vis
            .append("svg:text")
            .attr("class", "levelName")
            .attr("y", -50)
            .text("Total");

        this.levelDuration = this.vis.append("svg:text")
            .attr("class", "duration")
            .attr("y", -8);
        
        const path = this.vis
            .data([json])
            .selectAll("path")
            .data(nodes)
            .enter()
            .append("svg:path")
            .attr("display", d => d.depth ? null : "none")
            .attr("d", arc)
            .attr("fill-rule", "evenodd")
            .attr("class", d => this.getColorClass(d.name))
            .style("opacity", 1)
            .on("mouseover", d => this.mouseover(d));

        // Add the mouseleave handler to the bounding circle.
        this.vis
            .on("mouseleave", () => this.mouseleave());
        
        this.levelDuration
            .text(this.totalSize.toLocaleString() + " ms");
    }
    
    getColorClass(item: string) {
        return _.camelCase(item);
    }
    
    private mouseover(d: graphNode) {
        // Fade all but the current sequence, and show it in the breadcrumb trail.
        
        this.levelName
            .text(d.name);
        
        this.levelDuration
            .text(d.duration.toLocaleString() + " ms");
        
        const sequenceArray = this.getAncestors(d);
        
        // Fade all the segments.
        this.vis
            .selectAll("path")
            .style("opacity", 0.3);
        
        this.vis
            .selectAll("path")
            .transition()
            .duration(0)
            .filter(n => sequenceArray.indexOf(n) >= 0)
            .style("opacity", 1);
    }
    
    private mouseleave() {
        // Restore everything to full opacity when moving off the visualization.
        
        this.levelName
            .text("Total");
        
        this.levelDuration
            .text(this.totalSize.toLocaleString() + " ms");
        
        const self = this;
        
        this.vis.selectAll("path").on("mouseover", null);

        // Transition each segment to full opacity and then reactivate it.
        this.vis
            .selectAll("path")
            .transition()
            .duration(200)
            .style("opacity", 1)
            .each("end", function() {
                d3.select(this).on("mouseover", d => self.mouseover(d));
            });
    }
    
    private convertHierarchy(name: string, data: Raven.Client.Documents.Queries.Timings.QueryTimings): graphNode {
        const mappedTimings = _.map(data.Timings, (value, key) => this.convertHierarchy(key, value));
        const remainingTime = data.DurationInMs - _.sumBy(mappedTimings, x => x.duration);
        
        let children: Array<graphNode> = null;
        if (data.Timings) {
            children = mappedTimings;
            if (remainingTime > 0) {
                children.push({
                    name: timingsChart.fakeFill,
                    duration: remainingTime,
                    children: null as any,
                    visible: false
                });
            }
        }
        
        return {
            name: name,
            duration: data.DurationInMs,
            children: children as any,
            visible: true
        }
    }
    
    private getAncestors(node: graphNode) {
        // Given a node in a partition layout, return an array of all of its ancestor
        // nodes, highest first, but excluding the root.
        
        let path = [] as Array<graphNode>;
        let current = node;
        while (current.parent) {
            path.unshift(current);
            current = current.parent as graphNode;
        }
        return path;
    } 
    
    syncLegend() {
        const $query = $(".query");
        $query
            .on("mouseenter", ".timing-legend-item", (event: JQueryEventObject) => {
                const node = ko.dataFor(event.currentTarget) as graphNode;
                this.mouseover(node);
            })
            .on("mouseleave", ".timing-legend-item", () => this.mouseleave());
    }
}

export = timingsChart;
