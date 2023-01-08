import accessManager = require("common/shell/accessManager");
import getBucketsCommand = require("commands/database/debug/getBucketsCommand");
import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");
import generalUtils = require("common/generalUtils");
import d3 = require("d3");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";
import bucketReportItem from "models/database/status/bucketReportItem";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import getBucketCommand from "commands/database/debug/getBucketCommand";
import startReshardingCommand from "commands/database/dbGroup/startReshardingCommand";

type positionAndSizes = {
    dx: number,
    dy: number,
    x: number,
    y: number
}

type gridItem = { id: string };

class bucketsReport extends shardViewModelBase {

    view = require("views/database/status/bucketsReport.html");
    
    accessManager = accessManager.default.databasesView;

    static readonly animationLength = 200;
    static readonly maxChildrenToShow = 1000;

    private currentPath: KnockoutComputed<Array<bucketReportItem>>;

    private x: d3.scale.Linear<number, number>;
    private y: d3.scale.Linear<number, number>;
    private root: bucketReportItem;
    private node = ko.observable<bucketReportItem>();
    private treemap: d3.layout.Treemap<any>;
    private svg: d3.Selection<any>;
    private g: d3.Selection<any>;
    private tooltip: d3.Selection<any>;

    private w: number;
    private h: number;

    private transitioning = false;

    private gridController = ko.observable<virtualGridController<gridItem>>();
    showLoader = ko.observable<boolean>(false);
    
    showBucketContents = ko.observable<boolean>(false);

    constructor(db: database, location: databaseLocationSpecifier) {
        super(db, location);
        this.bindToCurrentInstance("onClick", "moveToDifferentShard");
    }

    activate(args: any) {
        super.activate(args);

        this.initObservables();

        return new getBucketsCommand(this.db)
            .execute()
            .done(result => {
                this.processData(result);
            });
    }

    compositionComplete() {
        super.compositionComplete();
        
        this.showBucketContents.subscribe(visible => {
            if (visible) {
                const grid = this.gridController();
                grid.headerVisible(true);

                grid.init(() => this.fetcher(), () => {
                    return [
                        new textColumn<gridItem>(grid, x => x.id, "Document ID", "90%"),
                    ];
                });
            }
        })

        this.initGraph();
        this.draw(undefined);
    }

    private fetcher(): JQueryPromise<pagedResult<gridItem>> {
        const bucket = this.node().fromRange;
        return new getBucketCommand(this.db, bucket)
            .execute()
            .then(result => {
                const ids = result.Documents.map(id => ({ id }));
                
                return {
                    items: ids,
                    totalResultCount: ids.length
                }
            });
    }

    private initObservables() {
        this.currentPath = ko.pureComputed(() => {
            const node = this.node();

            const items: bucketReportItem[] = [];

            let currentItem = node;
            while (currentItem) {
                items.unshift(currentItem);
                currentItem = currentItem.parent;
            }

            return items;
        });
    }

    private processData(data: Raven.Server.Web.Studio.Processors.BucketsResults) {
        const mappedData = Object.values<Raven.Server.Web.Studio.Processors.BucketRange>(data.BucketRanges).map(x => this.mapBucket(x));
        
        const totalSize = mappedData.reduce((p, c) => p + c.size, 0);
        const totalDocuments = mappedData.reduce((p, c) => p + c.documentsCount, 0);
        const totalBuckets = mappedData.reduce((p, c) => p + c.numberOfBuckets, 0);
        this.root = new bucketReportItem( "/", totalSize, totalBuckets, totalDocuments, [], mappedData);

        this.sortBySize(this.root);

        this.node(this.root);
    }

    private sortBySize(node: bucketReportItem) {
        if (node.internalChildren && node.internalChildren.length) {
            node.internalChildren.forEach(x => this.sortBySize(x));

            node.internalChildren.sort((a, b) => d3.descending(a.size, b.size));
        }
    }

    private mapBucket(dto: Raven.Server.Web.Studio.Processors.BucketRange): bucketReportItem {
        const leafBucket = dto.FromBucket === dto.ToBucket;
        const name = leafBucket ? "Bucket: " + dto.FromBucket : (dto.FromBucket + " - " + dto.ToBucket);
        const item = new bucketReportItem(name, dto.RangeSize, dto.NumberOfBuckets, dto.DocumentsCount, dto.ShardNumbers);
        item.fromRange = dto.FromBucket;
        item.toRange = dto.ToBucket;
        item.lazyLoadChildren = !leafBucket;
        const leafItem = new bucketReportItem("Bucket: " + dto.FromBucket, dto.RangeSize, dto.NumberOfBuckets, dto.DocumentsCount, dto.ShardNumbers);
        item.internalChildren = [leafItem];
        return item;
    }

    private initGraph() {
        this.detectContainerSize();
        this.x = d3.scale.linear().range([0, this.w]);
        this.y = d3.scale.linear().range([0, this.h]);

        this.svg = d3.select("#storage-report-container .chart")
            .append("svg:svg")
            .attr("width", this.w)
            .attr("height", this.h)
            .attr("transform", "translate(.5,.5)");
    }

    private detectContainerSize() {
        const $chartNode = $("#storage-report-container .chart");
        this.w = $chartNode.width();
        this.h = $chartNode.height();
    }

    private getChildren(node: bucketReportItem, depth: number) {
        return depth === 0 ? node.internalChildren : [];
    }

    private draw(goingIn: boolean, forceOldLocation?: positionAndSizes) {
        const levelDown = goingIn === true;
        const levelUp = goingIn === false;

        this.treemap = d3.layout.treemap<any>()
            .children((n, depth) => this.getChildren(n, depth))
            .value(d => d.size)
            .size([this.w, this.h]);

        this.tooltip = d3.select(".chart-tooltip");

        const oldLocation: positionAndSizes = forceOldLocation || {
            dx: this.node().dx,
            dy: this.node().dy,
            x: this.node().x,
            y: this.node().y
        };

        const nodes = this.treemap.nodes(this.node())
            .filter(n => !n.children);

        if (levelDown) {
            this.animateZoomIn(nodes, oldLocation);
        } else if (levelUp) {
            this.animateZoomOut(nodes);
        } else {
            // initial state
            this.svg.select(".treemap")
                .remove();
            const container = this.svg.append("g")
                .classed("treemap", true);
            this.drawNewTreeMap(nodes, container);
        }
    }

    private animateZoomIn(nodes: bucketReportItem[], oldLocation: positionAndSizes) {
        this.transitioning = true;

        const oldContainer = this.svg.select(".treemap");

        const newGroup = this.svg.append("g")
            .classed("treemap", true);

        const scaleX = this.w / oldLocation.dx;
        const scaleY = this.h / oldLocation.dy;
        const transX = -oldLocation.x * scaleX;
        const transY = -oldLocation.y * scaleY;

        oldContainer
            .selectAll("text")
            .transition()
            .duration(bucketsReport.animationLength / 4)
            .style('opacity', 0);

        oldContainer
            .transition()
            .duration(bucketsReport.animationLength)
            .attr("transform", "translate(" + transX + "," + transY + ")scale(" + scaleX + "," + scaleY + ")")
            .each("end", () => {
                const newCells = this.drawNewTreeMap(nodes, newGroup);
                newCells
                    .style('opacity', 0)
                    .transition()
                    .duration(bucketsReport.animationLength)
                    .style('opacity', 1)
                    .each("end", () => {
                        oldContainer.remove();
                        this.transitioning = false;
                    });
            });
    }

    private animateZoomOut(nodes: bucketReportItem[]) {
        this.transitioning = true;

        const oldContainer = this.svg.select(".treemap");

        const newGroup = this.svg.append("g")
            .classed("treemap", true);

        const newCells = this.drawNewTreeMap(nodes, newGroup);

        newCells
            .style('opacity', 0)
            .transition()
            .duration(bucketsReport.animationLength)
            .style('opacity', 1)
            .each("end", () => {
                oldContainer.remove();
                this.transitioning = false;
            });
    }

    private drawNewTreeMap(nodes: bucketReportItem[], container: d3.Selection<any>) {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;

        const cell = container.selectAll("g.cell-no-such") // we always select non-existing nodes to draw from scratch - we don't update elements here
            .data(nodes)
            .enter().append("svg:g")
            .attr("class", d => "cell index")
            .attr("transform", d => "translate(" + d.x + "," + d.y + ")")
            .on("click", d => this.onClick(d, true))
            .on("mouseover", d => this.onMouseOver(d))
            .on("mouseout", () => this.onMouseOut())
            .on("mousemove", () => this.onMouseMove());

        const rectangles = cell.append("svg:rect")
            .attr("width", d => Math.max(0, d.dx - 1))
            .attr("height", d => Math.max(0, d.dy - 1))

        rectangles
            .filter(x => x.hasChildren() || x.lazyLoadChildren)
            .style('cursor', 'pointer');

        cell.append("svg:text")
            .filter(d => d.dx > 20 && d.dy > 8)
            .attr("x", d => d.dx / 2)
            .attr("y", d => d.dy / 2)
            .attr("dy", ".35em")
            .attr("text-anchor", "middle")
            .text(d => d.name)
            .each(function (d) {
                self.wrap(this, d.dx);
            });
        
        return cell;
    }

    wrap($self: any, width: number) {
        const self = d3.select($self);
        let textLength = (self.node() as any).getComputedTextLength();
        let text = self.text();
        
        while (textLength > (width - 6) && text.length > 0) {
            text = text.slice(0, -1);
            self.text(text + '...');
            textLength = (self.node() as any).getComputedTextLength();
        }
    } 
    
    private loadDetailedReport(d: bucketReportItem): JQueryPromise<Raven.Server.Web.Studio.Processors.BucketsResults> {
        if (!d.lazyLoadChildren) {
            return;
        }

        const showLoaderTimer = setTimeout(() => {
            this.showLoader(true);
        }, 100);
        
        
        return new getBucketsCommand(this.db, {
            from: d.fromRange,
            to: d.toRange
        })
            .execute()
            .done(results => {
                d.lazyLoadChildren = false;
                d.internalChildren = Object.values<Raven.Server.Web.Studio.Processors.BucketRange>(results.BucketRanges).map(x => this.mapBucket(x));
            })
            .always(() => {
                if (this.showLoader()) {
                    this.showLoader(false);
                } else {
                    clearTimeout(showLoaderTimer);
                }
            });
    }
    
    private getNodePath(d: bucketReportItem): string[] {
        const result: string[] = [];
        
        do {
            result.push(d.name);
            d = d.parent;
        } while (d.name !== "/");
        
        return result.reverse();
    }
    
    private findNodeByPath(path: string[]): bucketReportItem {
        let target = this.root;

        for (const item of path) {
            target = target.internalChildren.find(x => x.name === item);

            if (!target) {
                return null;
            }
        }
        
        return target;
    }

    onClick(d: bucketReportItem, goingIn: boolean) {
        if (this.transitioning || this.node() === d) {
            return;
        }

        if (d.lazyLoadChildren) {
            const requestExecution = protractedCommandsDetector.instance.requestStarted(500);

            const nodePath = this.getNodePath(d);
            
            // copy old positions as node references might change during load (data overwrite)
            const oldLocation: positionAndSizes = {
                dx: d.dx,
                dy: d.dy,
                x: d.x,
                y: d.y
            };
            
            this.loadDetailedReport(d)
                .done(() => {
                    const nodeAfterLoad = this.findNodeByPath(nodePath);
                    
                    if (d !== nodeAfterLoad) {
                        // little magic here - we want to restore parent/child relations
                        d3.layout.treemap<any>()
                            .children((n) => n.internalChildren)
                            .value(d => d.size)
                            .size([this.w, this.h])
                            .nodes(this.root);
                    }
                    
                    this.sortBySize(nodeAfterLoad);
                    this.node(nodeAfterLoad);
                    
                    this.draw(true, oldLocation);
                    
                    this.maybeUpdateTable();
                })
                .always(() => requestExecution.markCompleted());

            if (d3.event) {
                (d3.event as any).stopPropagation();
            }
            return;
        }

        if (!d.internalChildren || !d.internalChildren.length) {
            return;
        }

        this.node(d);
        this.draw(goingIn);

        this.maybeUpdateTable();

        this.updateTooltips();
        
        if (d3.event) {
            (d3.event as any).stopPropagation();
        }
    }
    
    private maybeUpdateTable() {
        if (this.root !== this.node() && this.node().fromRange === this.node().toRange) {
            this.showBucketContents(true); 
            this.gridController().reset(true);
            
        } else {
            this.showBucketContents(false);
        }
    }
    
    private updateTooltips() {
        $('#storage-report [data-toggle="tooltip"]').tooltip();
    }

    private onMouseMove() {
        // eslint-disable-next-line prefer-const
        let [x, y] = d3.mouse(this.svg.node());

        const tooltipWidth = $(".chart-tooltip").width() + 20;

        x = Math.min(x, Math.max(this.w - tooltipWidth, 0));

        this.tooltip
            .style("left", (x + 10) + "px")
            .style("top", (y + 10) + "px");
    }

    private onMouseOver(d: bucketReportItem) {
        this.tooltip.transition()
            .duration(200)
            .style("opacity", 1);
        let html = "<div class='tooltip-li'>Range: <div class='value'>" + d.name + "</div></div>";
        html += "<div class='tooltip-li'>Documents Count: <div class='value'>" + d.documentsCount + "</div></div>";
        html += "<div class='tooltip-li'>Buckets Count: <div class='value'>" + d.numberOfBuckets + "</div></div>";
        html += "<div class='tooltip-li'>Shards: <div class='value'>" + d.shards.map(x => `<span># ${x}</span>`).join("") + "</div></div>";
        html += "<div class='tooltip-li'>Size: <div class='value'>" + generalUtils.formatBytesToSize(d.size) + "</div></div>";

        this.tooltip.html(html);
        this.onMouseMove();
    }

    private onMouseOut() {
        this.tooltip.transition()
            .duration(500)
            .style("opacity", 0);
    }

    moveToDifferentShard(item: bucketReportItem) {
        //TODO: provide better UI for getting shard number...
        const response = prompt("Enter shard#");
        const shardNumber = parseInt(response);
        new startReshardingCommand(this.db, { from: item.fromRange, to: item.toRange }, shardNumber)
            .execute();
    }
}

export = bucketsReport;
