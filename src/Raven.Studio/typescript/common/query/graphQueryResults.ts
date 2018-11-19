/// <reference path="../../../typings/tsd.d.ts" />

import app = require("durandal/app");
import cola = require("cola");
import document = require("models/database/documents/document");
import showDataDialog = require("viewmodels/common/showDataDialog");

interface debugGraphOutputNodeWithLayout extends debugGraphOutputNode, cola.Node {
    
}

interface debugGraphOutputEdge extends cola.Link<debugGraphOutputNodeWithLayout> {
    payload: any;
    name: string;
}

class graphQueryResults {
    
    public static readonly circleRadius = 40;
    public static readonly linkLength = 160;
    public static readonly clickDetectionRadius = 6;
    
    private width: number;
    private height: number;
    private svg: d3.Selection<void>;
    private zoom: d3.behavior.Zoom<void>;
    private d3cola: cola.D3StyleLayoutAdaptor;

    private edgesContainer: d3.Selection<void>;
    private nodesContainer: d3.Selection<void>;

    private readonly colorScale: d3.scale.Ordinal<string, string>;

    constructor(private selector: string) {
        this.colorScale = d3.scale.ordinal<string>()
            .range(["#f75e71", "#f38861", "#f0ae5e", "#edcd51", "#7bd85d", "#37c4ac", "#2f9ef3", "#6972ee", "#9457b5", "#d45598"]);
    }
    
    private init() {
        const $container = $(this.selector);
        $container.empty();
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

        this.svg.append("rect")
            .attr("class", "zoomRect")
            .attr("width", this.width)
            .attr("height", this.height)
            .style("fill", "none")
            .style("pointer-events", "all")
            .call(this.zoom)
            .on("dblclick.zoom", null);

        const transform = this.svg.append("g")
            .attr("class", "zoom")
            .attr("transform", "translate(" + (this.width / 2) + "," + (this.height / 2) + ")scale(1)");

        this.edgesContainer = transform.append("g")
            .attr("class", "edges");

        this.nodesContainer = transform.append("g")
            .attr("class", "nodes");

        this.d3cola = cola.d3adaptor();

        this.d3cola.on('tick', () => {
            this.updateElementDecorators();
        });

        this.svg.append("defs").append("marker")
            .attr({"id":"arrowhead",
                    "viewBox":"-0 -2.5 5 5",
                    "refX":25,
                    "refY":0,
                    "orient":"auto",
                    "markerWidth":5,
                    "markerHeight":5,
                    "xoverflow":"visible"})
            .append("svg:path")
            .attr("d", "M 0,-2.5 L 2.5 ,0 L 0,2.5")
            .attr("fill", "#ccc")
            .attr("stroke","#ccc");
    }

    private updateElementDecorators() {
        this.updateNodes(this.nodesContainer.selectAll(".node"));
        this.updateEdges(this.edgesContainer.selectAll(".edge"));
        this.updateEdgePaths(this.edgesContainer.selectAll(".edgePath"));
        this.updateEdgeLabels(this.edgesContainer.selectAll(".edgeLabel"));
    }

    private zoomed() {
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
    
    private getCollectionColor(data: debugGraphOutputNodeWithLayout) {
        const metadata = data.Value["@metadata"];
        if (!metadata) {
            return undefined;
        }
        
        return this.colorScale(metadata["@collection"]);
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
            .attr("class", "node")
            .call(this.d3cola.drag);

        let mouseDownPosition: [number, number] = null;
        let hasMouseDown = false; //used to control mouse button and down event
        
        enteringNodes
            .append("circle")
            .attr("class", "node-bg")
            .attr("r", 0)
            .attr("fill", d => this.getCollectionColor(d)) 
            .on("mousedown", e => {
                mouseDownPosition = d3.mouse(this.nodesContainer.node());
                hasMouseDown = (d3.event as MouseEvent).button === 0; // left mouse button
            })
            .on("mouseup", d => {
                if (hasMouseDown) {
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
            .append("text")
            .attr("class", "node-name")
            .text(x => x.Id)
            .attr("y", 5);

        const edges = this.edgesContainer
            .selectAll(".edge")
            .data(links, x => x.source.Id + "-" + x.target.Id);

        edges.exit()
            .remove();
        
        const enteringLines = edges
            .enter()
            .append("line");

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
            .attr({"d": d => "M " + d.source.x + " " + d.source.y + " L " + d.target.x + " " + d.target.y,
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
                "dx":60,
                "dy":0,
                "font-size":10,
                "fill":"#aaa"});

        edgeLabels.append("textPath")
            .attr("xlink:href",(d, i) => "#edgePath" + i)
            .style("pointer-events", "none")
            .text(d => d.name);
        
        const nodes = data.Nodes as Array<debugGraphOutputNodeWithLayout>;
        
        this.d3cola
            .linkDistance(() => graphQueryResults.linkLength)
            .nodes(nodes)
            .links(links)
            .avoidOverlaps(true)
            .start(30);

        enteringNodes
            .call(selection => this.updateNodes(selection));

        enteringLines
            .call(selection => this.updateEdges(selection));

        edgePaths
            .call(selection => this.updateEdgePaths(selection));
        
        edgeLabels
            .call(selection => this.updateEdgeLabels(selection));
        
        //TODO: set initial scale?
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
        
        return _.flatMap(data.Edges, edgesByType => {
           return edgesByType.Results.map(edge => {
               return {
                   source: nodesCache.get(edge.From),
                   target: nodesCache.get(edge.To),
                   name: edgesByType.Name,
                   payload: edge.Edge
               } as debugGraphOutputEdge;
           });
        }); 
    }

    private updateNodes(selection: d3.Selection<debugGraphOutputNodeWithLayout>) {
        selection
            .attr("transform", x => `translate(${x.x},${x.y})`);
    }

    private updateEdges(selection: d3.Selection<debugGraphOutputEdge>) {
        selection
            .attr("x1", x => x.source.x)
            .attr("y1", x => x.source.y)
            .attr("x2", x => x.target.x)
            .attr("y2", x => x.target.y);
    }
    
    private updateEdgePaths(selection: d3.Selection<debugGraphOutputEdge>) {
        selection
            .attr("d", d => "M " + d.source.x + " " + d.source.y + " L " + d.target.x + " " + d.target.y);
    }
    
    private updateEdgeLabels(selection: d3.Selection<debugGraphOutputEdge>) {
        selection.attr('transform',function(d,i){
            if (d.target.x < d.source.x){
                const bbox = this.getBBox();
                const rx = bbox.x+bbox.width/2;
                const ry = bbox.y+bbox.height/2;
                return "rotate(180 " + rx + " " + ry + ")";
            } else {
                return "rotate(0)";
            }
        });
    }
    
}

export = graphQueryResults;

