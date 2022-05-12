/// <reference path="../../../typings/tsd.d.ts" />

import app = require("durandal/app");
import document = require("models/database/documents/document");
import showDataDialog = require("viewmodels/common/showDataDialog");
import graphHelper = require("common/helpers/graph/graphHelper");
import { d3adaptor, Link, Node, ID3StyleLayoutAdaptor, Layout } from "webcola";
import d3 = require("d3");
import icomoonHelpers from "common/helpers/view/icomoonHelpers";

interface debugGraphOutputNodeWithLayout extends debugGraphOutputNode, Node {
    fixed: number;
}

interface debugGraphOutputEdge extends Link<debugGraphOutputNodeWithLayout> {
    payload: any;
    name: string;
    cacheKey: string;
    connectionNumber: number;
    totalConnections: number;
}

class graphQueryResults {
    
    public static readonly circleRadius = 40;
    public static readonly circleTextLineHeight = 14;
    public static readonly linkLength = 160;
    public static readonly clickDetectionRadius = 6;
    public static readonly circlePadding = 5;
    
    private width: number;
    private height: number;
    private svg: d3.Selection<void>;
    private zoom: d3.behavior.Zoom<void>;
    private d3cola: ID3StyleLayoutAdaptor & Layout;
    private mousePressed: boolean = false;

    private edgesContainer: d3.Selection<void>;
    private nodesContainer: d3.Selection<void>;

    private readonly colorClassScale: d3.scale.Ordinal<string, string>;

    constructor(private selector: string) {
        this.colorClassScale = d3.scale.ordinal<string>()
            .range(_.range(1, 11).map(x => "color-" + x));
    }
    
    clear() {
        const $container = $(this.selector);
        $container.empty();
    }
    
    private init() {
        this.clear();
        const $container = $(this.selector);
        const container = d3.select($container[0]);

        this.width = Math.floor($container.innerWidth());
        this.height = Math.floor($container.innerHeight());

        this.initGraph(container);
    }

    private initGraph(container: d3.Selection<any>) {
        this.zoom = d3.behavior.zoom<void>()
            .translate([this.width / 2, this.height / 2])
            .scale(1)
            .scaleExtent([0.25, 2])
            .on("zoom", () => this.zoomed());

        this.svg = container.append("svg")
            .style({
                width: this.width + "px",
                height: this.height + "px"
            })
            .attr("viewBox", "0 0 " + this.width + " " + this.height);

        const zoomContainer = this.svg.append("g")
            .attr("class", "container");
        
        zoomContainer.append("rect")
            .attr("class", "zoomRect")
            .attr("width", this.width)
            .attr("height", this.height)
            .style("fill", "none")
            .style("pointer-events", "all")
            .call(this.zoom)
            .on("dblclick.zoom", null);

        const transform = zoomContainer.append("g")
            .attr("class", "zoom")
            .attr("transform", "translate(" + (this.width / 2) + "," + (this.height / 2) + ")scale(1)");
            
        transform
            .on("mousedown", () => this.mousePressed = true)
            .on("mouseup", () => this.mousePressed = false);

        this.edgesContainer = transform.append("g")
            .attr("class", "edges");

        this.nodesContainer = transform.append("g")
            .attr("class", "nodes");

        this.d3cola = d3adaptor();

        this.d3cola.on('tick', () => {
            this.updateElementDecorators();
        });

        this.svg.append("defs").append("marker")
            .attr({"id":"arrowhead",
                    "refX": 5,
                    "refY": 3,
                    "orient":"auto",
                    "markerWidth":10,
                    "markerHeight":10,
                    "viewBox": "0 0 20 20",
                    "markerUnits": "strokeWidth",
                    "xoverflow":"visible"})
            .append("svg:path")
            .attr("d", "M0,0 L0,6 L9,3 z")
            .attr("fill", "#aaa")
            .attr("stroke","#aaa");
    }

    private updateElementDecorators() {
        this.updateNodes(this.nodesContainer.selectAll(".node"));
        this.updateEdges(this.edgesContainer.selectAll(".edge"));
        this.updateEdgePaths(this.edgesContainer.selectAll(".edgePath"));
    }

    private zoomed() {
        if (this.mousePressed) {
            return;
        }
        
        const event = d3.event as d3.ZoomEvent;
        this.svg
            .select(".zoom")
            .attr("transform", "translate(" + event.translate + ")scale(" + event.scale + ")");
    }
    
    stopSimulation() {
        if (this.d3cola) {
            this.d3cola.stop();
        }
    }
    
    private getCollectionColorClass(data: debugGraphOutputNodeWithLayout) {
        const metadata = data.Value["@metadata"];
        if (!metadata) {
            return undefined;
        }
        
        return this.colorClassScale(metadata["@collection"]);
    }
    
    draw(data: debugGraphOutputResponse) {
        this.init();

        const links = this.findLinksForCola(data);

        const graphNodes = this.nodesContainer
            .selectAll(".node")
            .data(data.Nodes as Array<debugGraphOutputNodeWithLayout>, x => x.Id);

        graphNodes.exit()
            .remove();
        
        const enteringNodes = graphNodes
            .enter()
            .append("g")
            .attr("class", "node");

        const nodes = data.Nodes as Array<debugGraphOutputNodeWithLayout>;

        this.d3cola
            .linkDistance(() => graphQueryResults.linkLength)
            .nodes(nodes)
            .links(links)
            .avoidOverlaps(true)
            .start(30, 0, 20);
        
        let mouseDownPosition: [number, number] = null;
        let hasMouseDown = false; //used to control mouse button and down event
        
        const self = this;
        
        enteringNodes
            .append("circle")
            .attr("class", d => "node-bg " + this.getCollectionColorClass(d))
            .attr("r", 0)
            .on("mousedown", function(d) {
                mouseDownPosition = d3.mouse(self.nodesContainer.node());
                const mouseEvent = d3.event as MouseEvent;
                hasMouseDown = mouseEvent.button === 0; // left mouse button
                
                // lock node position
                d.fixed = 1;
                d3.select(this.parentNode).classed("locked", true);
            })
            .on("mouseup", d => {
                if (hasMouseDown) {
                    const mouseEvent = d3.event as MouseEvent;
                    const upPosition = d3.mouse(this.nodesContainer.node());

                    const distanceSquared = Math.pow(mouseDownPosition[0] - upPosition[0], 2) + Math.pow(mouseDownPosition[1] - upPosition[1], 2);
                    if (Math.sqrt(distanceSquared) < graphQueryResults.clickDetectionRadius) {
                        this.showPreview(d);
                    }
                    hasMouseDown = false;
                }
            })
            .transition()
            .attr("r", graphQueryResults.circleRadius);
        
        enteringNodes
            .call(this.d3cola.drag);

        enteringNodes
            .append("text")
            .attr("class", "icon-style lock-icon")
            .html(icomoonHelpers.getCodePointForCanvas("lock"))
            .attr("y", 30);
        
        enteringNodes
            .append("text")
            .attr("class", "node-name")
            .text(x => x.Id)
            .attr("y", 5)
            .each(function (d, i) {
                const textWidth = this.getComputedTextLength();
                const maxWidth = 2 * (graphQueryResults.circleRadius - graphQueryResults.circlePadding);
                
                const numberOfLines = Math.ceil(textWidth / maxWidth);
                
                const textToPrint = numberOfLines > 3 ? d.Id.substr(0, Math.floor(3 * maxWidth * d.Id.length / textWidth)) : d.Id;
                
                if (numberOfLines > 1) {
                    const textNode = d3.select(this);
                    
                    const textToBreak = textToPrint;
                    const charactersPerLine = Math.ceil((textToBreak.length + 2) / numberOfLines);

                    textNode.text("");
                    
                    const lines: string[] = [];
                    for (let l = 0; l < numberOfLines; l++) {
                        textNode.append("tspan")
                            .text(textToBreak.substr(l * charactersPerLine, charactersPerLine))
                            .attr("x", 0)
                            .attr("dy", l === 0 ? 0 : 14);
                        lines.push();
                    } 
                    
                    textNode.attr("y", 5  - graphQueryResults.circleTextLineHeight * (numberOfLines - 1) / 2);
                }
            });

        const edges = this.edgesContainer
            .selectAll(".edge")
            .data(links, x =>  x.cacheKey + "@" + x.connectionNumber);

        edges.exit()
            .remove();
        
        const enteringLines = edges
            .enter()
            .append("path")
            .attr("d", d => graphHelper.quadraticBezierCurve(d.source, d.target, 0));

        enteringLines
            .attr('marker-end','url(#arrowhead)')
            .attr("opacity", 0)
            .attr("id", (d, i) => "edge" + i)
            .attr("class", "edge")
            .transition()
            .attr("opacity", 1);

        const edgePaths = this.edgesContainer.selectAll(".edgePath")
            .data(links)
            .enter()
            .append("path")
            .attr({
                "class":"edgePath",
                "fill-opacity":0,
                "stroke-opacity":0,
                "fill":"blue",
                "stroke":"red",
                "id":(d, i) => "edgePath" + i})
            .style("pointer-events", "none");

        const edgeLabels = this.edgesContainer.selectAll(".edgeLabel")
            .data(links)
            .enter()
            .append("text")
            .style("pointer-events", "none")
            .attr({"class":"edgeLabel",
                "id": (d, i) => "edgeLabel" + i,
                "dy":-2,
                "font-size":10,
                "fill":"#aaa"});

        edgeLabels.append("textPath")
            .attr("xlink:href",(d, i) => "#edgePath" + i)
            .attr("startOffset", "50%")
            .style("pointer-events", "none")
            .text(d => d.name);
        
        enteringNodes
            .call(selection => this.updateNodes(selection));

        enteringLines
            .call(selection => this.updateEdges(selection));

        edgePaths
            .call(selection => this.updateEdgePaths(selection));
    }

    private showPreview(data: debugGraphOutputNode) {
        const doc = new document(data.Value);
        const docDto = doc.toDto(true);

        const text = JSON.stringify(docDto, null, 4);
        const title = doc.getId() ? "Document: " + doc.getId() : "Document preview";
        app.showBootstrapDialog(new showDataDialog(title, text, "javascript"));
    }
    
    private findLinksForCola(data: debugGraphOutputResponse): Array<debugGraphOutputEdge> {
        const nodesCache = new Map<string, debugGraphOutputNodeWithLayout>();
        data.Nodes.forEach(node => {
            nodesCache.set(node.Id, node as debugGraphOutputNodeWithLayout);
        });
        
        const linkCardinalityCache = new Map<String, number>();
        
        const results = _.flatMap(data.Edges, edgesByType => {
           return edgesByType.Results.map((edge): debugGraphOutputEdge => {
               const cacheKey = edge.From + "->" + edge.To;
               const count = linkCardinalityCache.get(cacheKey) || 0;
               linkCardinalityCache.set(cacheKey, count + 1);
               
               return {
                   source: nodesCache.get(edge.From),
                   target: nodesCache.get(edge.To),
                   cacheKey: cacheKey,
                   name: edgesByType.Name,
                   payload: edge.Edge,
                   connectionNumber: count,
                   totalConnections: undefined
               };
           });
        }); 
        
        results.forEach(r => {
            r.totalConnections = linkCardinalityCache.get(r.cacheKey);
        });
        
        return results;
    }

    private updateNodes(selection: d3.Selection<debugGraphOutputNodeWithLayout>) {
        selection
            .attr("transform", x => `translate(${x.x},${x.y})`);
    }

    private updateEdges(selection: d3.Selection<debugGraphOutputEdge>) {
        selection
            .attr("d", d => { 
                const delta = this.getDelta(d);
                const shift = Math.sign(delta) * 7;
                const [source, target] = graphHelper.movePoints(d.source, d.target, shift);
                
                return graphHelper.quadraticBezierCurve(source, target, delta, graphQueryResults.circleRadius + graphQueryResults.circlePadding);
            });
    }
    
    private updateEdgePaths(selection: d3.Selection<debugGraphOutputEdge>) {
        selection
            .attr("d", d => {
                const delta = this.getDelta(d);
                const shift = Math.sign(delta) * 7;
                const [source, target] = graphHelper.movePoints(d.source, d.target, shift);

                if (target.x > source.x) {
                    return graphHelper.quadraticBezierCurve(source, target, delta, graphQueryResults.circleRadius + graphQueryResults.circlePadding);
                } else {
                    return graphHelper.quadraticBezierCurve(target, source, -delta, graphQueryResults.circleRadius + graphQueryResults.circlePadding);
                }
            });
    }
    
    private getDelta(d: debugGraphOutputEdge): number {
        return 30 * (d.connectionNumber + 0.5 - d.totalConnections / 2);
    }
    
    onResize() {
        if (!this.svg) {
            // svg is not yet initialized 
            return;
        }
        const $container = $(this.selector);

        this.width = Math.floor($container.innerWidth());
        this.height = Math.floor($container.innerHeight());

        this.svg
            .style({
                width: this.width + "px",
                height: this.height + "px"
            })
            .attr("viewBox", "0 0 " + this.width + " " + this.height);

        this.svg.select(".zoomRect")
            .attr("width", this.width)
            .attr("height", this.height);
    }
    
}

export = graphQueryResults;

