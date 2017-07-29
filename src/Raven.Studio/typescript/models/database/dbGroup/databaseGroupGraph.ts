/// <reference path="../../../../typings/tsd.d.ts"/>

import d3 = require("d3");
import graphHelper = require("common/helpers/graph/graphHelper");
import cola = require("cola");


abstract class layoutable {
    x: number;
    y: number;
    width: number;
    height: number;
    number: number;
    fixed: boolean;

    abstract getId(): string;
}

class databaseNode extends layoutable {
    tag: string;
    type: clusterNodeType;

    private constructor() {
        super();
    }

    getId() {
        return `d_${this.tag}`;
    }

    static for(dto: Raven.Client.Server.Operations.NodeId, type: clusterNodeType) {
        const node = new databaseNode();
        node.updateWith(dto, type);
        return node;
    }

    updateWith(dto: Raven.Client.Server.Operations.NodeId, type: clusterNodeType) {
        this.tag = dto.NodeTag;
        this.type = type;
    }
}

class taskNode extends layoutable {
    type: Raven.Client.Server.Operations.OngoingTaskType;
    taskId: number;
    name: string;

    responsibleNode: databaseNode;

    private constructor() {
        super();
    }

    static for(dto: Raven.Server.Web.System.OngoingTask, responsibleNode: databaseNode) {
        const node = new taskNode();
        node.updateWith(dto, responsibleNode);
        return node;
    }

    updateWith(dto: Raven.Server.Web.System.OngoingTask, responsibleNode: databaseNode) {
        this.type = dto.TaskType;
        this.taskId = dto.TaskId;
        //TODO: add node status 
        this.name = dto.TaskName;
        this.responsibleNode = responsibleNode;
    }

    getId() {
        return `t_${this.type}_${this.taskId}`;
    }
    
}

//TODO: introduce taskgroupnode

class databaseGroupGraph {

    private static readonly circleRadius = 42;
    private static readonly minDatabaseGroupDrawRadius = 80;
    private static readonly minDistanceBetweenCirclesInDatabaseGroup = 120;

    private data = {
        databaseNodes: [] as Array<databaseNode>,
        tasks: [] as Array<taskNode>
    }
    
    private $container: JQuery;
    private width: number;
    private height: number;
    private svg: d3.Selection<void>;
    private zoom: d3.behavior.Zoom<void>;
    private d3cola: cola.D3StyleLayoutAdaptor;
    private colaInitialized = false;
    private graphInitialized = false;
    private previousDbNodesCount = -1;

    private ongoingTasksCache: Raven.Server.Web.System.OngoingTasksResult;
    private databaseInfoCache: Raven.Client.Server.Operations.DatabaseInfo;

    private edgesContainer: d3.Selection<void>;
    private tasksContainer: d3.Selection<void>;
    private dbNodesContainer: d3.Selection<void>;

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
            .attr("width", this.width)
            .attr("height", this.height)
            .style("fill", "none")
            .style("pointer-events", "all")
            .call(this.zoom);

        //TODO: update scale after initial draw
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
            this.updateDbNodes(this.dbNodesContainer.selectAll(".db-node"));
            this.updateTaskNodes(this.tasksContainer.selectAll(".task-node"));
            this.updateEdges(this.edgesContainer.selectAll(".edge"));
        });

        this.graphInitialized = true;
        this.draw();
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

        //TODO: review which items should be updated during small redraw
        
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
            if (canUpdatePositions) {
                n.x = n.responsibleNode.x * 2;
                n.y = n.responsibleNode.y * 2;
            }
            n.width = 160; //TOOO dynamic  - approx. based on text length + ellipsis
            n.height = 40;
            n.number = idx + dbNodes.length;
        });

        const distanceCache = new Map<string, number>();

        links.forEach(link => {
            if (link.source instanceof databaseNode) {
                if (link.target instanceof databaseNode) {
                    //TODO: this doesn't respect min circle size
                    link.length = databaseGroupGraph.minDistanceBetweenCirclesInDatabaseGroup;
                } else {
                    const cacheKey = link.source.getId();
                    let currentValue = distanceCache.get(cacheKey) || 0;
                    
                    // after each 8 outgoing links from given node exand ideal link width by 20 px
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

        const links = this.findLinks(this.data.databaseNodes, this.data.tasks);

        this.layout(this.data.databaseNodes, this.data.tasks, links, !this.colaInitialized);

        //TODO instead of fixing positions of nodes try to set equality constraints
        
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
            .append("rect")
            .attr("class", "node-bg")
            .attr("width", x => x.width) //TODO: move to update ? 
            .attr("height", x => x.height)
            .call(this.d3cola.drag); 

        enteringTaskNodes
            .append("text")
            .attr("class", "task-desc")
            .attr("x", 47)
            .attr("y", 35);
        
        enteringTaskNodes
            .append("text")
            .attr("x", 10)
            .attr("y", 33)
            .attr("class", "icon-style task-icon");

        enteringTaskNodes.append("text")
            .attr("class", "task-name")
            .text(x => x.name)
            .attr("y", 18)
            .attr("x", 45);

        const edgeNodes = this.edgesContainer
            .selectAll(".edge")
            .data(links, x => x.source.getId() + "-" + x.target.getId());

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

        this.d3cola
            .nodes(nodes)
            .linkDistance(x => x.length)
            .links(links)
            .avoidOverlaps(false);
        
        if (this.colaInitialized) {
            this.d3cola.start(0, 0, 0, 0, true);
            this.d3cola.alpha(2);
            this.d3cola.resume();
        } else {
            this.d3cola
                .start(0, 0, 30, 0, true);
            this.colaInitialized = true;
        }

        enteringDbNodes
            .call(selection => this.updateDbNodes(selection));

        enteringTaskNodes
            .call(selection => this.updateTaskNodes(selection));

        enteringLines
            .call(selection => this.updateEdges(selection));
    }

    private findLinks(dbNodes: Array<databaseNode>, tasks: Array<taskNode>): Array<cola.Link<databaseNode | taskNode>> {
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

    private updateDbNodes(selection: d3.Selection<databaseNode>) {
        selection
            .attr("class", x => "db-node " + x.type)
            .attr("transform", x => `translate(${x.x},${x.y})`);

        const nodeIcon = (node: databaseNode) => {
            switch (node.type) {
                case "Member":
                    return "&#xe9c0;";
                case "Promotable":
                    return "&#xe9c1;";
                case "Watcher":
                    return "&#xe9c2;";
            }
            return "";
        };

        selection
            .select(".node-icon")
            .html(x => nodeIcon(x));
    }

    private updateTaskNodes(selection: d3.Selection<taskNode>) {

        //TODO: update type  + width + state
        selection
            .attr("class", x => "task-node " + x.type);

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
            }
            return "";
        };

        selection
            .select(".task-name")
            .text(x => x.name); //TODO: maybe trim

        selection
            .select(".task-desc")
            .text(x => x.type);
        
        selection
            .select(".task-icon")
            .html(x => taskIcon(x));
        
        selection
            .attr("transform", x => `translate(${x.x - x.width / 2},${x.y - x.height / 2})`);
    }

    private updateEdges(selection: d3.Selection<cola.Link<cola.Node>>) {
        //TODO: update type - up/down etc.
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

    onDatabaseInfoChanged(dbInfo: Raven.Client.Server.Operations.DatabaseInfo) {
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
        
        const merge = (nodes: Array<Raven.Client.Server.Operations.NodeId>, type: clusterNodeType) => {
            nodes.forEach(node => {
                const existing = this.data.databaseNodes.find(x => x.tag === node.NodeTag);
                if (existing) {
                    existing.updateWith(node, type);
                } else {
                    this.data.databaseNodes.push(databaseNode.for(node, type));
                }

                newDbTags.push(node.NodeTag);
            });
        }

        merge(this.databaseInfoCache.NodesTopology.Members, "Member");
        merge(this.databaseInfoCache.NodesTopology.Promotables, "Promotable");

        const dbsToDelete = this.data.databaseNodes.filter(x => !_.includes(newDbTags, x.tag));

        _.pullAll(this.data.databaseNodes, dbsToDelete);

        _.sortBy(this.data.databaseNodes, x => x.tag);
    }

    private updateTasks() {
        const newTasksIds = this.ongoingTasksCache.OngoingTasksList.map(x => x.TaskId);

        this.ongoingTasksCache.OngoingTasksList.forEach(taskDto => {
            const responsibleNode = taskDto.ResponsibleNode ? this.data.databaseNodes.find(x => x.tag === taskDto.ResponsibleNode.NodeTag) : null;
            
            const existing = this.data.tasks.find(x => x.taskId === taskDto.TaskId);
            if (existing) {
                existing.updateWith(taskDto, responsibleNode);
            } else {
                this.data.tasks.push(taskNode.for(taskDto, responsibleNode));
            }
        });

        
        const tasksToDelete = this.data.tasks.filter(x => !_.includes(newTasksIds, x.taskId));
        _.pullAll(this.data.tasks, tasksToDelete);
        
        //TODO: process subscripitons - bundled for now
    }

}

export = databaseGroupGraph;
