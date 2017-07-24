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

    private static readonly circleRadius = 42;
    private static readonly drawRadius = 170;

    private $container: JQuery;
    private width: number;
    private height: number;
    private svg: d3.Selection<void>;
    private nodesGroup: d3.Selection<void>;
    private edgesGroup: d3.Selection<void>;
    private nodes: d3.Selection<clusterNode>;
    private edges: d3.Selection<string>;

    init(container: JQuery) {
        this.$container = container;
        this.width = container.innerWidth();
        this.height = container.innerHeight();

        this.initGraph();
    }

    private initGraph() {
        const container = d3.select(this.$container[0]);
        this.svg = container.append("svg")
            .style({
                width: this.width + "px",
                height: this.height + "px"
            })
            .attr("viewBox", "0 0 " + this.width + " " + this.height);


        const defs = this.svg
            .append("defs");

        const tranform = this.svg.append("g")
            .attr("transform", "translate(" + (this.width / 2) + "," + (this.height / 2) + ")");

        this.nodesGroup = tranform.append("g")
            .attr("class", "nodes");

        this.edgesGroup = tranform.append("g")
            .attr("class", "edges");
    }

    draw(nodes: clusterNode[], leaderTag: string) {
        this.layout(nodes);

        const edges = this.extractEdges(nodes as clusterNodeWithLayout[]);

        this.drawNodes(nodes, leaderTag);
        this.drawEdges(edges, leaderTag);
    }

    private drawNodes(nodes: clusterNode[], leaderTag: string) {
        const isPassive = nodes.length === 1 && nodes[0].tag() === "?";

        this.nodesGroup
            .classed("passive", isPassive)
            .classed("voting", !leaderTag);

        this.edgesGroup
            .classed("voting", !leaderTag);

        const nodesBinding = this.nodesGroup
            .selectAll(".node")
            .data(nodes as clusterNodeWithLayout[], x => x.tag());

        nodesBinding
            .call(selection => this.updateNodes(selection, leaderTag));

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
            .attr("r", clusterGraph.circleRadius + 6);

        enteringGroups
            .append("text")
            .attr("class", "node-tag")
            .text(x => x.tag() === "?" ? "" : x.tag()) // dont display '?' as tag, when passive
            .attr("y", 25);

        enteringGroups
            .append("text")
            .attr("class", "state")
            .attr("y", 1);

        enteringGroups
            .append("text")
            .attr("class", "icon-style node-icon");

        enteringGroups
            .call(selection => this.updateNodes(selection, leaderTag));
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

    private updateNodes(selection: d3.Selection<clusterNodeWithLayout>, leaderTag: string) {
        selection
            .attr("class", x => "node " + x.type() + (x.tag() === leaderTag ? " leader" : "") + (x.connected() ? "" : " disconnected"))
            .transition()
            .attr("transform", x => `translate(${x.x},${x.y})`);

        const nodeIcon = (node: clusterNodeWithLayout) => {
            if (node.tag() === leaderTag) {
                return "&#xe959;";
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
            if (node.tag() === "?") {
                return "PASSIVE";
            }
            return "VOTING";
        }

        selection
            .select(".state")
            .text(x => stateProvider(x));
    }

    private updateEdges(selection: d3.Selection<clusterGraphEdge<clusterNodeWithLayout>>, leaderTag: string) {
        const edgeDistance = 50;

        selection
            .select(".edge-line")
            .classed("with-watcher", x => x.source.type() === "Watcher" || x.target.type() === "Watcher")
            .classed("with-leader", x => x.source.tag() === leaderTag || x.target.tag() === leaderTag)
            .classed("with-error", x => !x.source.connected() || !x.target.connected())
            .transition() 
            .attr("x1", x => graphHelper.shortenLineFromObject(x, edgeDistance).x1)
            .attr("y1", x => graphHelper.shortenLineFromObject(x, edgeDistance).y1)
            .attr("x2", x => graphHelper.shortenLineFromObject(x, edgeDistance).x2)
            .attr("y2", x => graphHelper.shortenLineFromObject(x, edgeDistance).y2)
            .style('opacity', 1);

    }

    private layout(nodes: clusterNode[]) {
        const layoutableNodes = nodes as Array<clusterNodeWithLayout>;
        graphHelper.circleLayout(layoutableNodes, clusterGraph.drawRadius);

    }
}

export = clusterGraph;
