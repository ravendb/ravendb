/// <reference path="../../../typings/tsd.d.ts" />

import app = require("durandal/app");
import cola = require("cola");
import document = require("models/database/documents/document");
import showDataDialog = require("viewmodels/common/showDataDialog");

interface debugGraphOutputNodeWithLayout extends debugGraphOutputNode, cola.Node {
    
}

class graphQueryResults {
    
    public static readonly circleRadius = 40;
    public static readonly linkLength = 120;
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
    }

    private updateElementDecorators() {
        this.updateNodes(this.nodesContainer.selectAll(".node"));
        this.updateEdges(this.edgesContainer.selectAll(".edge"));
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
        
        enteringNodes
            .append("circle")
            .attr("class", "node-bg")
            .attr("r", 0)
            .attr("fill", d => this.getCollectionColor(d))
            .on("mousedown", () => {
                mouseDownPosition = d3.mouse(this.nodesContainer.node());
            })
            .on("mouseup", d => {
                const upPosition = d3.mouse(this.nodesContainer.node());
                
                const distanceSquared = Math.pow(mouseDownPosition[0] - upPosition[0], 2) + Math.pow(mouseDownPosition[1] - upPosition[1], 2);
                if (Math.sqrt(distanceSquared) < graphQueryResults.clickDetectionRadius) {
                    this.showPreview(d);
                }
            })
            .transition()
            .attr("r", graphQueryResults.circleRadius);

        enteringNodes
            .append("text")
            .attr("class", "node-name")
            .text(x => x.Id)
            .attr("y", 5);

        const edgeNodes = this.edgesContainer
            .selectAll(".edge")
            .data(links, x => x.source.Id + "-" + x.target.Id);

        edgeNodes.exit()
            .remove();
        
        const enteringLines = edgeNodes
            .enter()
            .append("line");

        enteringLines
            .attr("opacity", 0)
            .attr("class", "edge")
            .transition()
            .attr("opacity", 1);

        const nodes = data.Nodes as Array<debugGraphOutputNodeWithLayout>;
        
        this.d3cola
            .linkDistance(() => graphQueryResults.linkLength)
            .nodes(nodes)
            .links(links)
            .start(30);

        enteringNodes
            .call(selection => this.updateNodes(selection));

        enteringLines
            .call(selection => this.updateEdges(selection));
        
        //TODO: set initial scale?
    }

    private showPreview(data: debugGraphOutputNode) {
        const doc = new document(data.Value);
        const docDto = doc.toDto(true);

        const text = JSON.stringify(docDto, null, 4);
        const title = doc.getId() ? "Document: " + doc.getId() : "Document preview";
        app.showBootstrapDialog(new showDataDialog(title, text, "javascript"));
    }
    
    private findLinksForCola(data: debugGraphOutputResponse): Array<cola.Link<debugGraphOutputNodeWithLayout>> {
        const nodesCache = new Map<string, debugGraphOutputNodeWithLayout>();
        data.Nodes.forEach(node => {
            nodesCache.set(node.Id, node as debugGraphOutputNodeWithLayout);
        });
        
        return _.flatMap(data.Edges, e => 
            e.To.map(to => {
                return {
                    source: nodesCache.get(e.From),
                    target: nodesCache.get(to),
                } as cola.Link<debugGraphOutputNodeWithLayout>;
            }));
    }

    private updateNodes(selection: d3.Selection<debugGraphOutputNodeWithLayout>) {
        selection
            .attr("transform", x => `translate(${x.x},${x.y})`);
    }

    private updateEdges(selection: d3.Selection<cola.Link<debugGraphOutputNodeWithLayout>>) {
        selection
            .attr("x1", x => x.source.x)
            .attr("y1", x => x.source.y)
            .attr("x2", x => x.target.x)
            .attr("y2", x => x.target.y);
    }
}

export = graphQueryResults;

