/// <reference path="../../../../typings/tsd.d.ts"/>
import d3 = require("d3");
import graphHelper = require("common/helpers/graph/graphHelper");
import cola = require("cola");
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");


abstract class layoutable {
    x: number;
    y: number;
    width: number;
    height: number;
    number: number;
    fixed: boolean;

    abstract getId(): string;
}

type databaseNodeState = "valid" | "errored";

class databaseNode extends layoutable {
    tag: string;
    type: databaseGroupNodeType;
    responsibleNode: string;
    status: Raven.Client.ServerWide.DatabasePromotionStatus;
    
    private constructor() {
        super();
    }

    getId() {
        return `d_${this.tag}`;
    }

    static for(dto: Raven.Client.ServerWide.Operations.NodeId, type: databaseGroupNodeType) {
        const node = new databaseNode();
        node.updateWith(dto, type);
        return node;
    }

    updateWith(dto: Raven.Client.ServerWide.Operations.NodeId, type: databaseGroupNodeType) {
        this.tag = dto.NodeTag;
        this.type = type;
        this.responsibleNode = dto.ResponsibleNode;
    }

    getStateClass() {
        switch (this.status) {
            case "NotResponding":
                return "state-danger";
            case "Ok":
                return "state-success";
            default:
                return "state-warning";
        }
    }
}

class taskNode extends layoutable {
    static readonly maxWidth = 270;
    static readonly minWidth = 170;
    static readonly textLeftPadding = 45;
    
    type: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;
    taskId: number;
    uniqueId: string;
    name: string;
    state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

    responsibleNode: databaseNode;

    private constructor() {
        super();
    }

    static for(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask, responsibleNode: databaseNode) {
        const node = new taskNode();
        node.updateWith(dto, responsibleNode);
        return node;
    }

    updateWith(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask, responsibleNode: databaseNode) {
        this.type = dto.TaskType;
        this.uniqueId = databaseGroupGraph.getUniqueTaskId(dto);
        this.taskId = dto.TaskId;
        this.state = dto.TaskState;
        this.name = dto.TaskName;
        this.responsibleNode = responsibleNode;
    }

    getId() {
        return `t_${this.type}_${this.uniqueId}`;
    }
}

interface taskNodeWithCache extends taskNode {
    trimmedName: string;
}

class databaseGroupGraph {

    private static readonly circleRadius = 42;
    private static readonly minDatabaseGroupDrawRadius = 80;
    private static readonly minDistanceBetweenCirclesInDatabaseGroup = 120;

    private data = {
        databaseNodes: [] as Array<databaseNode>,
        tasks: [] as Array<taskNode>
    };
    
    private $container: JQuery;
    private width: number;
    private height: number;
    private svg: d3.Selection<void>;
    private zoom: d3.behavior.Zoom<void>;
    private d3cola: cola.D3StyleLayoutAdaptor;
    private colaInitialized = false;
    private graphInitialized = false;
    private previousDbNodesCount = -1;
    private previousLinks: string = null;

    private ongoingTasksCache: Raven.Server.Web.System.OngoingTasksResult;
    private databaseInfoCache: Raven.Client.ServerWide.Operations.DatabaseInfo;

    private edgesContainer: d3.Selection<void>;
    private tasksContainer: d3.Selection<void>;
    private dbNodesContainer: d3.Selection<void>;

    private savedWidthAndHeight: [number, number] = null;

    constructor() {
        _.bindAll(this, ...["enterFullScreen", "exitFullScreen"] as Array<keyof this>);
    }

    init(container: JQuery) {
        this.$container = container;
        this.width = container.innerWidth();
        this.height = container.innerHeight();
        
        this.initGraph();
    }

    private initGraph() {
        const container = d3.select(this.$container[0]);

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
            .call(this.zoom);

        const tranform = this.svg.append("g")
            .attr("class", "zoom")
            .attr("transform", "translate(" + (this.width / 2) + "," + (this.height / 2) + ")scale(1)");

        this.edgesContainer = tranform.append("g")
            .attr("class", "edges");

        this.tasksContainer = tranform.append("g")
            .attr("class", "task-nodes");

        this.dbNodesContainer = tranform.append("g")
            .attr("class", "db-nodes");

        this.d3cola = cola.d3adaptor();

        this.d3cola.on('tick', () => {
            this.updateElementDecorators();
        });

        this.graphInitialized = true;
        this.draw();
    }
    
    private updateElementDecorators() {
        this.updateDbNodes(this.dbNodesContainer.selectAll(".db-node"));
        this.updateTaskNodes(this.tasksContainer.selectAll(".task-node"));
        this.updateEdges(this.edgesContainer.selectAll(".edge"));
    }

    private zoomed() {
        const event = d3.event as d3.ZoomEvent;
        this.svg
            .select(".zoom")
            .attr("transform", "translate(" + event.translate + ")scale(" + event.scale + ")");
    }

    private calculateDbGroupRadius(nodesCount: number) {
        return Math.max(databaseGroupGraph.minDatabaseGroupDrawRadius, Math.floor(databaseGroupGraph.minDistanceBetweenCirclesInDatabaseGroup * nodesCount / (2 * Math.PI)));
    }

    private layout(dbNodes: Array<databaseNode>, tasks: Array<taskNode>, links: Array<cola.Link<databaseNode | taskNode>>, canUpdatePositions: boolean) {

        dbNodes.forEach((n, idx) => {
            n.width = 2 * databaseGroupGraph.circleRadius;
            n.height = 2 * databaseGroupGraph.circleRadius;
            n.number = idx;
            n.fixed = true;
        });

        const radius = this.calculateDbGroupRadius(dbNodes.length);

        // allow to recalculate layout only for a first time
        // or if amount of nodes changed
        if (canUpdatePositions || this.previousDbNodesCount !== dbNodes.length) {
            graphHelper.circleLayout(dbNodes, radius);
        }
        
        tasks.forEach((n, idx) => {
            if (canUpdatePositions && n.responsibleNode) {
                n.x = n.responsibleNode.x * 2;
                n.y = n.responsibleNode.y * 2;
            }
            n.height = 40;
            n.number = idx + dbNodes.length;
        });

        const distanceCache = new Map<string, number>();

        links.forEach(link => {
            if (link.source instanceof databaseNode) {
                if (link.target instanceof databaseNode) {
                    // formula for size length (a) based on circumscribed circle diameter (R)
                    // for regular polygon edges count (n)
                    // a = 2 * R * sin (Pi / n)
                    link.length = 2 * radius * Math.sin(Math.PI / dbNodes.length);
                } else {
                    const cacheKey = link.source.getId();
                    let currentValue = distanceCache.get(cacheKey) || 0;
                    
                    // after each 8 outgoing links from given node extend ideal link width by 20 px
                    link.length = 140 + Math.floor(currentValue / 8) * 20;

                    currentValue++;
                    distanceCache.set(cacheKey, currentValue);
                }
            } else {
                throw new Error("Can not compile link length, source: " + link.source + " -> " + link.target);
            }
        });

        this.previousDbNodesCount = dbNodes.length;
    }

    draw() {
        if (!this.databaseInfoCache || !this.ongoingTasksCache || !this.graphInitialized) {
            return;
        }

        const colaLinks = this.findLinksForCola(this.data.databaseNodes, this.data.tasks);

        const visibleLinks = this.findVisibleLinks(this.data.databaseNodes, this.data.tasks);
        
        const linksEncoded = this.getLinksEncoded(colaLinks);
        
        this.layout(this.data.databaseNodes, this.data.tasks, colaLinks, !this.colaInitialized);

        const dbNodes = this.dbNodesContainer
            .selectAll(".db-node")
            .data(this.data.databaseNodes, x => x.getId());

        dbNodes.exit()
            .remove();
        
        const enteringDbNodes = dbNodes
            .enter()
            .append("g")
            .attr("class", "db-node");

        enteringDbNodes
            .append("circle")
            .attr("class", "node-bg")
            .attr("r", 0)
            .call(this.d3cola.drag)
            .transition()
            .attr("r", databaseGroupGraph.circleRadius);

        enteringDbNodes
            .append("circle")
            .attr("class", "catching-up-stroke spin-style-noease")
            .attr("r", databaseGroupGraph.circleRadius + 4);
        
        enteringDbNodes
            .append("text")
            .attr("class", "node-tag")
            .text(x => x.tag)
            .attr("y", 25);

        enteringDbNodes
            .append("text")
            .attr("class", "icon-style node-icon");

        const taskNodes = this.tasksContainer
            .selectAll(".task-node")
            .data(this.data.tasks, x => x.getId());

        taskNodes.exit()
            .remove();

        const enteringTaskNodes = taskNodes
            .enter()
            .append("g")
            .attr("class", "task-node")
            .attr("opacity", 0);
        
        enteringTaskNodes
            .transition()
            .delay((d) => d.number * 20 + 200)
            .attr("opacity", 1);

        enteringTaskNodes
            .append("title");
        
        enteringTaskNodes
            .append("rect")
            .attr("class", "node-bg")
            .call(this.d3cola.drag); 

        enteringTaskNodes
            .append("text")
            .attr("class", "task-desc")
            .attr("x", taskNode.textLeftPadding + 2)
            .attr("y", 35);
        
        enteringTaskNodes
            .append("text")
            .attr("x", 10)
            .attr("y", 33)
            .attr("class", "icon-style task-icon");

        enteringTaskNodes.append("text")
            .attr("class", "task-name")
            .attr("y", 18)
            .attr("x", taskNode.textLeftPadding);

        const edgeNodes = this.edgesContainer
            .selectAll(".edge")
            .data(visibleLinks, x => x.source.getId() + "-" + x.target.getId());

        edgeNodes.exit()
            .remove();
        
        const enteringLines = edgeNodes
            .enter()
            .append("line");

        enteringLines
            .attr("opacity", 0)
            .attr("class", "edge")
            .attr("data-id", x => x.source.getId() + "-" + x.target.getId())
            .transition()
            .delay(d => d.target.number * 20 + 200)
            .attr("opacity", 1);

        const nodes = ([] as cola.Node[]).concat(this.data.databaseNodes, this.data.tasks);

        this.tasksContainer
            .selectAll(".task-node")
            .select(".task-name")
            .text(x => x.name)
            .call(selection => {
                selection.each(function (x: taskNodeWithCache) {
// ReSharper disable once SuspiciousThisUsage
                    const textNode = this as SVGTextElement;
                    const trimmed = graphHelper.trimText(x.name, l => textNode.getSubStringLength(0, l), taskNode.minWidth - taskNode.textLeftPadding, taskNode.maxWidth - taskNode.textLeftPadding, 10);
                    x.width = trimmed.containerWidth + taskNode.textLeftPadding; // add some extra padding
                    x.trimmedName = trimmed.text;
                });
            });

        this.d3cola
            .nodes(nodes)
            .linkDistance(x => x.length)
            .links(colaLinks)
            .avoidOverlaps(false);

        if (this.colaInitialized) {
            
            if (this.previousLinks !== linksEncoded) {
                this.d3cola.start(0, 0, 0, 0, true);
                this.d3cola.alpha(2);
                this.d3cola.resume();
            } else {
                this.updateElementDecorators();
            }
            
        } else {
            this.d3cola
                .start(0, 0, 30, 0, true);
        }

        enteringDbNodes
            .call(selection => this.updateDbNodes(selection));

        enteringTaskNodes
            .call(selection => this.updateTaskNodes(selection));

        enteringLines
            .call(selection => this.updateEdges(selection));

        if (!this.colaInitialized) {

            this.zoom
                .scale(this.calculateInitialScale());

            this.svg
                .select(".zoom")
                .call(this.zoom.event);
            this.colaInitialized = true;
        }
        
        this.previousLinks = linksEncoded;
    }

    private calculateInitialScale() {
        const bbox = (this.svg
            .select(".zoom")
            .node() as SVGGraphicsElement)
            .getBBox();

        let scale = 1;
        const percentagePadding = 0.1;

        const maybeReduceScale = (actualSize: number, maxSize: number) => {
            const maxSizeWithPadding = maxSize * (1.0 - percentagePadding);
            if (Math.abs(actualSize) > maxSizeWithPadding) {
                scale = Math.min(scale, maxSizeWithPadding / Math.abs(actualSize));
            }
        };

        maybeReduceScale(bbox.x, this.width / 2);
        maybeReduceScale(bbox.x + bbox.width, this.width / 2);
        maybeReduceScale(bbox.y, this.height / 2);
        maybeReduceScale(bbox.y + bbox.height, this.height / 2);
        
        return scale;
    }

    // link used in forge graph simulation
    private findLinksForCola(dbNodes: Array<databaseNode>, tasks: Array<taskNode>): Array<cola.Link<databaseNode | taskNode>> {
        const links = [] as Array<cola.Link<databaseNode | taskNode>>;

        for (let i = 0; i < dbNodes.length; i++) {
            links.push({
                source: dbNodes[i],
                target: dbNodes[(i + 1) % dbNodes.length],
            });
        }
        
        tasks.forEach((task) => {
            if (task.responsibleNode) {
                links.push({
                    source: task.responsibleNode,
                    target: task,
                });
            }
        });

        return links;
    }

    // link displayed on scroon
    private findVisibleLinks(dbNodes: Array<databaseNode>, tasks: Array<taskNode>): Array<cola.Link<databaseNode | taskNode>> {
        const links = [] as Array<cola.Link<databaseNode | taskNode>>;

        // find member - member connections

        for (let i = 0; i < dbNodes.length; i++) {
            for (let j = 0; j < dbNodes.length; j++) {
                if (i !== j) {
                    const source = dbNodes[i];
                    const target = dbNodes[j];

                    if (source.type === "Member" && target.type === "Member") {
                        links.push({
                            source: source,
                            target: target
                        });
                    }
                }
            }
        }

        // find promotable/rehab -> member links

        for (let i = 0; i < dbNodes.length; i++) {
            const node = dbNodes[i];
            if (node.type !== "Member" && node.responsibleNode) {
                const responsibleNode = dbNodes.find(x => x.tag === node.responsibleNode);

                links.push({
                    source: node,
                    target: responsibleNode
                });
            }
        }
        

        tasks.forEach((task) => {
            if (task.responsibleNode) {
                links.push({
                    source: task.responsibleNode,
                    target: task,
                });
            }
        });

        return links;
    }

    private updateDbNodes(selection: d3.Selection<databaseNode>) {
        selection
            .attr("class", x => "db-node " + x.type + " " + x.getStateClass())
            .attr("transform", x => `translate(${x.x},${x.y})`);

        const nodeIcon = (node: databaseNode) => {
            switch (node.type) {
                case "Member":
                    return "&#xe9c0;";
                case "Promotable":
                    return "&#xe9c1;";
                case "Rehab":
                    return "&#xe9c4;";
            }
            return "";
        };

        selection
            .select(".node-icon")
            .html(x => nodeIcon(x));
    }

    private updateTaskNodes(selection: d3.Selection<taskNode>) {
        selection
            .attr("class", x => "task-node " + x.type + " " + x.state);

        const taskIcon = (node: taskNode) => {
            switch (node.type) {
                case "Backup":
                    return "&#xe9b6;";
                case "RavenEtl":
                    return "&#xe9b8;";
                case "Replication":
                    return "&#xe9b7;";
                case "SqlEtl":
                    return "&#xe9b9;";
                case "Subscription":
                    return "&#xe9b5;";
                case "PullReplicationAsHub":
                    return "&#xea1c;";
                case "PullReplicationAsSink":
                    return "&#xea1b;";
            }
            return "";
        };

        selection
            .select("title")
            .text(x => x.name);
        
        selection
            .select(".node-bg")
            .attr("width", x => x.width)
            .attr("height", x => x.height);

        selection
            .select(".task-desc")
            .text(x => x.type);

        selection
            .select(".task-name")
            .text((x: taskNodeWithCache) => x.trimmedName);
        
        selection
            .select(".task-icon")
            .html(x => taskIcon(x));
        
        selection
            .attr("transform", x => `translate(${x.x - x.width / 2},${x.y - x.height / 2})`);
    }

    private updateEdges(selection: d3.Selection<cola.Link<taskNode | databaseNode>>) {
        selection.attr("class", x => "edge " + ((x.target instanceof taskNode) ? x.target.state : " "));

        
        selection.classed("errored", x => {
            if (x.source instanceof databaseNode && x.target instanceof databaseNode) {
                return x.source.getStateClass() === "state-danger" || x.target.getStateClass() === "state-danger";
            }

            return false;
        });

        selection.classed("warning", x => {
            if (x.source instanceof databaseNode && x.target instanceof databaseNode) {
                return x.source.getStateClass() === "state-warning" || x.target.getStateClass() === "state-warning";
            }

            return false;
        });
        
        selection
            .attr("x1", x => x.source.x)
            .attr("y1", x => x.source.y)
            .attr("x2", x => x.target.x)
            .attr("y2", x => x.target.y);
    }

    onTasksChanged(taskInfo: Raven.Server.Web.System.OngoingTasksResult) {
        this.ongoingTasksCache = taskInfo;
        this.updateData();
        this.draw();
    }

    onDatabaseInfoChanged(dbInfo: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        this.databaseInfoCache = dbInfo;
        this.updateData();
        this.draw();
    }

    updateData() {
        if (!this.databaseInfoCache || !this.ongoingTasksCache) {
            return;
        }

        this.updateDatabaseNodes();
        this.updateTasks();
    }

    private updateDatabaseNodes() {
        const newDbTags = [] as Array<string>;
        
        const merge = (nodes: Array<Raven.Client.ServerWide.Operations.NodeId>, type: databaseGroupNodeType) => {
            nodes.forEach(node => {
                const existing = this.data.databaseNodes.find(x => x.tag === node.NodeTag);
                if (existing) {
                    existing.updateWith(node, type);
                } else {
                    this.data.databaseNodes.push(databaseNode.for(node, type));
                }

                newDbTags.push(node.NodeTag);
            });
        };

        merge(this.databaseInfoCache.NodesTopology.Members, "Member");
        merge(this.databaseInfoCache.NodesTopology.Promotables, "Promotable");
        merge(this.databaseInfoCache.NodesTopology.Rehabs, "Rehab");

        const dbsToDelete = this.data.databaseNodes.filter(x => !_.includes(newDbTags, x.tag));

        _.pullAll(this.data.databaseNodes, dbsToDelete);

        _.sortBy(this.data.databaseNodes, x => x.tag);

        // clear current status
        this.data.databaseNodes.forEach(node => {
            node.status = "Ok"; 
        });
        
        _.forIn(this.databaseInfoCache.NodesTopology.Status, (status, tag) => {
            const matchingNode = this.data.databaseNodes.find(x => x.tag === tag);
            //TODO: update this logic once RavenDB-7998 will be completed
            if (matchingNode) {
                matchingNode.status = status.LastStatus;
            }
        });
    }
    
    static getUniqueTaskId(task: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask): string {
        if (task.TaskType === "PullReplicationAsHub") {
            const hubTask = task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub;
            // since hub is generic definition we need to differentiate between instances
            return hubTask.TaskId + ":" + hubTask.DestinationDatabase + "@" + hubTask.DestinationUrl;
        }
        
        return task.TaskId.toString();
    }

    private updateTasks() {
        const newTasksIds = this.ongoingTasksCache.OngoingTasksList.map(x => databaseGroupGraph.getUniqueTaskId(x));
        
        this.ongoingTasksCache.OngoingTasksList.forEach(taskDto => {
            const responsibleNode = taskDto.ResponsibleNode ? this.data.databaseNodes.find(x => x.tag === taskDto.ResponsibleNode.NodeTag) : null;
            
            const taskUniqueId = databaseGroupGraph.getUniqueTaskId(taskDto);
            
            const existing = this.data.tasks.find(x => x.uniqueId === taskUniqueId);
            if (existing) {
                existing.updateWith(taskDto, responsibleNode);
            } else {
                this.data.tasks.push(taskNode.for(taskDto, responsibleNode));
            }
        });

        const tasksToDelete = this.data.tasks.filter(x => !_.includes(newTasksIds, x.uniqueId));
        _.pullAll(this.data.tasks, tasksToDelete);
    }
     
    onResize() {
        if ($(document).fullScreen()) {
            this.width = screen.width;
            this.height = screen.height;
        } else {
            [this.width, this.height] = this.savedWidthAndHeight;
            this.savedWidthAndHeight = null;
        }

        this.svg
            .style({
                width: this.width + "px",
                height: this.height + "px"
            })
            .attr("viewBox", "0 0 " + this.width + " " + this.height);
        
        this.svg.select(".zoomRect")
            .attr("width", this.width)
            .attr("height", this.height); 
        
        this.zoom.translate([this.width / 2, this.height / 2]);

        this.svg
            .select(".zoom")
            .call(this.zoom.event);
    }

    enterFullScreen() {
        this.savedWidthAndHeight = [this.width, this.height];
        
        $("#databaseGroupGraphContainer").fullScreen(true);
    }

    exitFullScreen() {
        $("#databaseGroupGraphContainer").fullScreen(false);
    }
    
    private getLinksEncoded(links: Array<cola.Link<databaseNode | taskNode>>) {
        const result = [] as Array<string>;
        links.forEach(link => {
            const linkEndpoints = [link.source.getId(), link.target.getId()];
            linkEndpoints.sort();
            result.push(linkEndpoints.join("<->"));
        });
        
        result.sort();
        
        return result.join(",");
    }

}

export = databaseGroupGraph;
