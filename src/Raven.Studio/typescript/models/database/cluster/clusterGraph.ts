/// <reference path="../../../../typings/tsd.d.ts"/>

import d3 = require("d3");
import clusterNode = require("models/database/cluster/clusterNode");
import graphHelper = require("common/helpers/graph/graphHelper");

interface clusterNodeWithLayout extends clusterNode {
    x: number;
    y: number;
}

class clusterGraphEdge<T extends  clusterNode> {
    constructor(public source: T, public target: T) {
        
    }

    getId() {
        return this.source.tag() + "_" + this.target.tag();
    }
}

class clusterGraph {

    private static readonly circleRadius = 50;
    private static readonly minDrawRadius = 170;
    private static readonly minDistanceBetweenCircles = 145;

    private $container: JQuery;
    private width: number;
    private height: number;
    private svg: d3.Selection<void>;
    private zoom: d3.behavior.Zoom<void>;
    private nodesGroup: d3.Selection<void>;
    private edgesGroup: d3.Selection<void>;
    private nodes: d3.Selection<clusterNode>;
    private edges: d3.Selection<string>;

    init(container: JQuery, initialNodesCount: number) {
        this.$container = container;
        this.width = container.innerWidth();
        this.height = container.innerHeight();

        this.initGraph(initialNodesCount);
    }

    private initialScale(initialNodesCount: number) {
        const estimatedRadius = this.estimatedRadius(initialNodesCount);
        const availableSpace = Math.min(this.width, this.height);

        if (2 * estimatedRadius > availableSpace * 0.8) {
            return availableSpace * 0.8 / (estimatedRadius * 2);
        } else {
            // it will fit
            return 1;
        }
    }

    private initGraph(initialNodesCount: number) {
        const container = d3.select(this.$container[0]);

        const initialScale = this.initialScale(initialNodesCount);

        this.zoom = d3.behavior.zoom<void>()
            .translate([this.width / 2, this.height / 2])
            .scale(initialScale)
            .scaleExtent([0.25, 2])
            .on("zoom", () => this.zoomed());

        this.svg = container.append("svg")
            .style({
                width: this.width + "px",
                height: this.height + "px"
            })
            .attr("viewBox", "0 0 " + this.width + " " + this.height)
            .call(this.zoom);

        this.svg.append("rect")
            .attr("width", this.width)
            .attr("height", this.height)
            .style("fill", "none")
            .style("pointer-events", "all");

        const tranform = this.svg.append("g")
            .attr("class", "zoom")
            .attr("transform", "translate(" + (this.width / 2) + "," + (this.height / 2) + ")scale(" + initialScale + ")");

        this.nodesGroup = tranform.append("g")
            .attr("class", "nodes");

        this.edgesGroup = tranform.append("g")
            .attr("class", "edges");
    }

    private zoomed() {
        const event = d3.event as d3.ZoomEvent;
        this.svg
            .select(".zoom")
            .attr("transform", "translate(" + event.translate + ")scale(" + event.scale + ")");
    }

    draw(nodes: clusterNode[], leaderTag: string, isPassive: boolean) {
        this.layout(nodes);

        const edges = this.extractEdges(nodes as clusterNodeWithLayout[]);

        this.drawNodes(nodes, leaderTag, isPassive);
        this.drawEdges(edges, leaderTag);
    }

    private drawNodes(nodes: clusterNode[], leaderTag: string, isPassive: boolean) {
        this.nodesGroup
            .classed("passive", isPassive)
            .classed("voting", !leaderTag);

        this.edgesGroup
            .classed("voting", !leaderTag);

        const nodesBinding = this.nodesGroup
            .selectAll(".node")
            .data(nodes as clusterNodeWithLayout[], x => x.tag());

        nodesBinding
            .call(selection => this.updateNodes(selection, leaderTag, isPassive));

        nodesBinding
            .exit()
            .remove();

        const enteringGroups = nodesBinding
            .enter()
            .append("g")
            .attr('class', "node");

        enteringGroups
            .append("circle")
            .attr("class", "node-bg")
            .attr("r", clusterGraph.circleRadius);

        enteringGroups
            .append("circle")
            .attr("class", "leader-stroke")
            .attr("r", clusterGraph.circleRadius + 4);

        enteringGroups
            .append("circle")
            .attr("class", "voting-stroke spin-style-noease")
            .attr("r", clusterGraph.circleRadius + 4);

        enteringGroups
            .append("text")
            .attr("class", "node-tag")
            .text(x => x.tag() === "?" ? "" : x.tag()) // dont display '?' as tag, when passive
            .attr("y", 26);

        enteringGroups
            .append("text")
            .attr("class", "state")
            .attr("y", 1);

        enteringGroups
            .append("text")
            .attr("class", "icon-style node-icon");

        enteringGroups
            .call(selection => this.updateNodes(selection, leaderTag, isPassive));
    }

    private drawEdges(edges: clusterGraphEdge<clusterNodeWithLayout>[], leaderTag: string) {
        const edgesBinding = this.edgesGroup
            .selectAll(".edge")
            .data(edges, x => x.getId());

        edgesBinding
            .call(selection => this.updateEdges(selection, leaderTag));

        edgesBinding
            .exit()
            .remove();

        const enteringEdges = edgesBinding
            .enter()
            .append("g")
            .attr("class", "edge");

        enteringEdges
            .append("line")
            .attr("class", "edge-line")
            .style('opacity', 0)
            .attr("x1", x => x.source.x)
            .attr("y1", x => x.source.y)
            .attr("x2", x => x.target.x)
            .attr("y2", x => x.target.y)
            .transition()
            .style('opacity', 1);
            

        enteringEdges
            .call(selection => this.updateEdges(selection, leaderTag));
    }

    private extractEdges(nodes: clusterNodeWithLayout[]) {
        const edges = [] as Array<clusterGraphEdge<clusterNodeWithLayout>>;

        graphHelper.pairIterator(nodes, (a, b) => {
            if (a.tag() < b.tag()) {
                edges.push(new clusterGraphEdge<clusterNodeWithLayout>(a, b));
            }
        });

        return edges;
    }

    private updateNodes(selection: d3.Selection<clusterNodeWithLayout>, leaderTag: string, isPassive: boolean) {
        selection
            .attr("class", x => "node " + x.type() + (x.tag() === leaderTag ? " leader" : "") + (x.connected() ? "" : " disconnected"))
            .transition()
            .attr("transform", x => `translate(${x.x},${x.y})`);

        const nodeIcon = (node: clusterNodeWithLayout) => {
            if (node.tag() === leaderTag) {
                return "&#xe9d5;";
            }

            switch (node.type()) {
                case "Member":
                    return "&#xe9ca;";
                case "Promotable":
                    return "&#xe9cc;";
                case "Watcher":
                    return leaderTag ? "&#xe9cd;" : "&#xe9a9;";
            }
            return "";
        };

        selection
            .select(".node-icon")
            .html(x => nodeIcon(x));

        const stateProvider = (node: clusterNodeWithLayout) => {
            if (leaderTag) {
                return "";
            }
            if (node.type() === "Watcher") {
                return "WAITING";
            }
            if (isPassive) {
                return "PASSIVE";
            }
            return "VOTING";
        };

        selection
            .select(".state")
            .text(x => stateProvider(x));
    }

    private updateEdges(selection: d3.Selection<clusterGraphEdge<clusterNodeWithLayout>>, leaderTag: string) {
        const leaderDistance = 52;
        const nonLeaderDistance = 45;

        selection
            .select(".edge-line")
            .classed("with-watcher", x => x.source.type() === "Watcher" || x.target.type() === "Watcher")
            .classed("with-leader", x => x.source.tag() === leaderTag || x.target.tag() === leaderTag)
            .classed("with-error", x => !x.source.connected() || !x.target.connected())
            .transition() 
            .attr("x1", x => graphHelper.shortenLineFromObject(x, x.target.tag() === leaderTag ? leaderDistance : nonLeaderDistance).x1)
            .attr("y1", x => graphHelper.shortenLineFromObject(x, x.target.tag() === leaderTag ? leaderDistance : nonLeaderDistance).y1)
            .attr("x2", x => graphHelper.shortenLineFromObject(x, x.source.tag() === leaderTag ? leaderDistance : nonLeaderDistance).x2)
            .attr("y2", x => graphHelper.shortenLineFromObject(x, x.source.tag() === leaderTag ? leaderDistance : nonLeaderDistance).y2)
            .style('opacity', 1);

    }

    private estimatedRadius(nodesCount: number) {
        return Math.floor(clusterGraph.minDistanceBetweenCircles * nodesCount / (2 * Math.PI));
    }

    private layout(nodes: clusterNode[]) {
        const layoutableNodes = nodes as Array<clusterNodeWithLayout>;
        const radius = Math.max(clusterGraph.minDrawRadius, this.estimatedRadius(nodes.length));
        graphHelper.circleLayout(layoutableNodes, radius);

    }
}

export = clusterGraph;
