import router = require("plugins/router");
import appUrl = require("common/appUrl");
import accessManager = require("common/shell/accessManager");
import getStorageReportCommand = require("commands/database/debug/getStorageReportCommand");
import getEnvironmentStorageReportCommand = require("commands/database/debug/getEnvironmentStorageReportCommand");
import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");
import generalUtils = require("common/generalUtils");
import storageReportItem = require("models/database/status/storageReportItem");
import d3 = require("d3");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";

type positionAndSizes = {
    dx: number,
    dy: number,
    x: number,
    y: number
}

class storageReport extends shardViewModelBase {

    view = require("views/database/status/storageReport.html");
    
    canCompactDatabase = accessManager.default.isOperatorOrAbove

    static readonly animationLength = 200;
    static readonly maxChildrenToShow = 1000;

    basePath: string;
    private rawData: storageReportItemDto[] = [];

    private currentPath: KnockoutComputed<Array<storageReportItem>>;

    private x: d3.scale.Linear<number, number>;
    private y: d3.scale.Linear<number, number>;
    private root: storageReportItem;
    private node = ko.observable<storageReportItem>();
    private treemap: d3.layout.Treemap<any>;
    private svg: d3.Selection<any>;
    private g: d3.Selection<any>;
    private tooltip: d3.Selection<any>;

    private w: number;
    private h: number;

    private transitioning = false;

    showLoader = ko.observable<boolean>(false);
    showPagesColumn: KnockoutObservable<boolean>;
    showEntriesColumn: KnockoutObservable<boolean>;
    showTempFiles: KnockoutObservable<boolean>;

    documentsCompressionUrl: KnockoutComputed<string>;

    constructor(db: database, location: databaseLocationSpecifier) {
        super(db, location);
        this.bindToCurrentInstance("onClick");
    }

    activate(args: any) {
        super.activate(args);

        this.initObservables();

        return new getStorageReportCommand(this.db, this.location)
            .execute()
            .done(result => {
                this.basePath = result.BasePath;
                this.rawData = result.Results;
            });
    }

    compositionComplete() {
        super.compositionComplete();

        this.processData();
        this.initGraph();
        this.draw(undefined);
    }

    private initObservables() {
        this.currentPath = ko.pureComputed(() => {
            const node = this.node();

            const items: storageReportItem[] = [];

            let currentItem = node;
            while (currentItem) {
                items.unshift(currentItem);
                currentItem = currentItem.parent;
            }

            return items;
        });

        this.showEntriesColumn = ko.pureComputed(() => {
            const node = this.node();
            return !!node.internalChildren.find(x => x.type === "table" || x.type === "tree");
        });

        this.showPagesColumn = ko.pureComputed(() => {
            const node = this.node();
            return !!node.internalChildren.find(x => x.type === "tree");
        });
        
        this.showTempFiles = ko.pureComputed(() => {
            return this.node() == this.root;
        })

        this.documentsCompressionUrl = ko.pureComputed(() => appUrl.forDocumentsCompression(this.db));
    }

    private processData() {
        const data = this.rawData;

        const mappedData = data.map(x => this.mapReport(x));
        const totalSize = mappedData.reduce((p, c) => p + c.size, 0);
        const item = new storageReportItem("/", "Database", false, totalSize, mappedData);

        this.root = item;

        this.sortBySize(this.root);

        this.node(this.root);
    }

    private sortBySize(node: storageReportItem) {
        if (node.internalChildren && node.internalChildren.length) {
            node.internalChildren.forEach(x => this.sortBySize(x));

            node.internalChildren.sort((a, b) => d3.descending(a.size, b.size));
        }
    }

    private mapReport(reportItem: storageReportItemDto): storageReportItem {
        const dataFile = this.mapDataFile(reportItem.Report);
        const journals = this.mapJournals(reportItem.Report);
        const tempFiles = this.mapTempFiles(reportItem.Report);

        return new storageReportItem(reportItem.Name,
            reportItem.Type.toLowerCase(),
            storageReport.showDisplayReportType(reportItem.Type),
            dataFile.size + journals.size + tempFiles.size,
            [dataFile, journals, tempFiles]);
    }

    private static showDisplayReportType(reportType: string): boolean {
        return reportType !== "Configuration" && reportType !== "Subscriptions";
    }

    private mapDataFile(report: Voron.Debugging.StorageReport): storageReportItem {
        const dataFile = report.DataFile;

        const storageItem = new storageReportItem("Datafile", "data", false, dataFile.AllocatedSpaceInBytes);
        storageItem.lazyLoadChildren = true;

        return storageItem;
    }

    private mapDetailedReport(report: Voron.Debugging.DetailedStorageReport, d: storageReportItem) {
        d.lazyLoadChildren = false;

        const tables = this.mapTables(report.Tables);
        const trees = this.mapTrees(report.Trees, "Trees");
        const freeSpace = new storageReportItem("Free", "free", false, report.DataFile.FreeSpaceInBytes, []);
        const preallocatedBuffers = this.mapPreAllocatedBuffers(report.PreAllocatedBuffers);

        d.internalChildren = [tables, trees, freeSpace, preallocatedBuffers];
    }

    private mapPreAllocatedBuffers(buffersReport: Voron.Debugging.PreAllocatedBuffersReport): storageReportItem {
        const allocationTree = this.mapTree(buffersReport.AllocationTree);
        const buffersSpace = new storageReportItem("Pre Allocated Buffers Space", "reserved", false, buffersReport.PreAllocatedBuffersSpaceInBytes);
        buffersSpace.pageCount = buffersReport.NumberOfPreAllocatedPages;

        const preAllocatedBuffers = new storageReportItem("Pre Allocated Buffers", "reserved", false, buffersReport.AllocatedSpaceInBytes, [allocationTree, buffersSpace]);
        preAllocatedBuffers.customSizeProvider = (header: boolean) => {
            const allocatedSizeFormatted = generalUtils.formatBytesToSize(buffersReport.AllocatedSpaceInBytes);
            if (header) {
                return allocatedSizeFormatted;
            }
            const originalSizeFormatted = generalUtils.formatBytesToSize(buffersReport.OriginallyAllocatedSpaceInBytes);
            return `<span title="${allocatedSizeFormatted} available out of ${originalSizeFormatted} reserved">${allocatedSizeFormatted} (out of ${originalSizeFormatted})</span>`;
        };
        return preAllocatedBuffers;
    }
    
    private groupChildren(items: storageReportItem[]): storageReportItem[] {
        if (items.length > storageReport.maxChildrenToShow) {
            items.sort((a, b) => b.size - a.size);

            const sampleItem = items[0];

            const itemsToShow = items.slice(0, storageReport.maxChildrenToShow);
            const itemsToGroup = items.slice(storageReport.maxChildrenToShow);
            
            let totalSize = 0;
            let totalLength = 0;
            let totalPageCount = 0;
            let totalNumberOfEntries = 0;

            for (const item of itemsToGroup) {
                totalSize += item.size;
                totalLength += item.length;
                totalPageCount += item.pageCount;
                totalNumberOfEntries += item.numberOfEntries;
            }

            const groupedItem = new storageReportItem("Grouped Data", sampleItem.type, sampleItem.showType, totalSize, [], true);
            groupedItem.length = totalLength;
            groupedItem.pageCount = totalPageCount;
            groupedItem.numberOfEntries = totalNumberOfEntries;
            
            itemsToShow.unshift(groupedItem);
            return itemsToShow;
        }
        
        return items;
    }

    private mapTables(tables: Voron.Data.Tables.TableReport[]): storageReportItem {
        const mappedTables = tables.map(x => this.mapTable(x));
        const mappedTablesGrouped = this.groupChildren(mappedTables);
        
        return new storageReportItem("Tables", "tables", false, mappedTables.reduce((p, c) => p + c.size, 0), mappedTablesGrouped);
    }

    private mapTable(table: Voron.Data.Tables.TableReport): storageReportItem {
        const structure = this.mapTrees(table.Structure, "Structure");

        const data = new storageReportItem("Table Data", "table_data", false, table.DataSizeInBytes, []);
        const indexes = this.mapTrees(table.Indexes, "Indexes");

        const preAllocatedBuffers = this.mapPreAllocatedBuffers(table.PreAllocatedBuffers);

        const totalSize = table.AllocatedSpaceInBytes;

        const tableItem = new storageReportItem(table.Name, "table", true, totalSize, [
            structure,
            data,
            indexes,
            preAllocatedBuffers
        ]);

        tableItem.numberOfEntries = table.NumberOfEntries;

        return tableItem;
    }

    private mapTrees(trees: Voron.Debugging.TreeReport[], name: string): storageReportItem {
        const mappedTrees = trees.map(x => this.mapTree(x));
        const mappedTreesGrouped = this.groupChildren(mappedTrees);
        
        return new storageReportItem(name, name.toLowerCase(), false, trees.reduce((p, c) => p + c.AllocatedSpaceInBytes, 0), mappedTreesGrouped);
    }

    private mapTree(tree: Voron.Debugging.TreeReport): storageReportItem {
        const children = (tree.Streams && tree.Streams.Streams) ? this.mapStreams(tree.Streams.Streams) : [];
        
        const item = new storageReportItem(tree.Name, "tree", true, tree.AllocatedSpaceInBytes, children);
        
        item.pageCount = tree.PageCount;
        item.numberOfEntries = tree.NumberOfEntries;
        item.lazyLoadChildren = children.length === 1 && children[0].name === "Stream details summary info";
        return item;
    }

    private mapStreams(treeStreams: Array<Voron.Debugging.StreamDetails>): storageReportItem[] {
        const mappedTreeStreams = treeStreams.map(x => this.mapStream(x));
        const mappedTreeStreamsGrouped = this.groupChildren(mappedTreeStreams);
        
        mappedTreeStreamsGrouped.forEach(x => x.customSizeProvider = (header: boolean) => {
            const allocatedSizeFormatted = generalUtils.formatBytesToSize(x.size);
            if (header) {
                return allocatedSizeFormatted;
            }
            const length = generalUtils.formatBytesToSize(x.length);
            return `<span title="stream length: ${length} / total allocation: ${allocatedSizeFormatted}">${length} / ${allocatedSizeFormatted}</span>`;
        });
        
        return mappedTreeStreamsGrouped;
    }

    private mapStream(stream: Voron.Debugging.StreamDetails): storageReportItem {
        const streamItem = new storageReportItem(stream.Name, "stream", false, stream.AllocatedSpaceInBytes, []);
        streamItem.length = stream.Length;
        return streamItem;
    }

    private mapJournals(report: Voron.Debugging.StorageReport): storageReportItem {
        const journals = report.Journals;

        const mappedJournals = journals.map(journal => 
            new storageReportItem(
                "Journal #" + journal.Number,
                "journal",
                false,
                journal.AllocatedSpaceInBytes,
                []
            ));

        return new storageReportItem("Journals", "journals", false, mappedJournals.reduce((p, c) => p + c.size, 0), mappedJournals);
    }
    
    private mapTempFiles(report: Voron.Debugging.StorageReport): storageReportItem {
        const tempFiles = report.TempFiles;

        const mappedTemps = tempFiles.map(temp => {
            const item = new storageReportItem(
                temp.Name,
                "temp",
                false,
                temp.AllocatedSpaceInBytes,
                []
            );
            
            item.recyclableJournal = temp.Type === "RecyclableJournal";
            
            return item;
        });

        return new storageReportItem("Temporary Files", "tempFiles", false, mappedTemps.reduce((p, c) => p + c.size, 0), mappedTemps);
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

    private getChildren(node: storageReportItem, depth: number) {
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

    private animateZoomIn(nodes: storageReportItem[], oldLocation: positionAndSizes) {
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
            .duration(storageReport.animationLength / 4)
            .style('opacity', 0);

        oldContainer
            .transition()
            .duration(storageReport.animationLength)
            .attr("transform", "translate(" + transX + "," + transY + ")scale(" + scaleX + "," + scaleY + ")")
            .each("end", () => {
                const newCells = this.drawNewTreeMap(nodes, newGroup);
                newCells
                    .style('opacity', 0)
                    .transition()
                    .duration(storageReport.animationLength)
                    .style('opacity', 1)
                    .each("end", () => {
                        oldContainer.remove();
                        this.transitioning = false;
                    });
            });
    }

    private animateZoomOut(nodes: storageReportItem[]) {
        this.transitioning = true;

        const oldContainer = this.svg.select(".treemap");

        const newGroup = this.svg.append("g")
            .classed("treemap", true);

        const newCells = this.drawNewTreeMap(nodes, newGroup);

        newCells
            .style('opacity', 0)
            .transition()
            .duration(storageReport.animationLength)
            .style('opacity', 1)
            .each("end", () => {
                oldContainer.remove();
                this.transitioning = false;
            });
    }

    private drawNewTreeMap(nodes: storageReportItem[], container: d3.Selection<any>) {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        const showTypeOffset = 7;
        const showTypePredicate = (d: storageReportItem) => d.showType && d.dy > 22 && d.dx > 20;

        const cell = container.selectAll("g.cell-no-such") // we always select non-existing nodes to draw from scratch - we don't update elements here
            .data(nodes)
            .enter().append("svg:g")
            .attr("class", d => "cell " + d.type + (d.isGrouped ? " grouped" : ""))
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
            .attr("y", d => showTypePredicate(d) ? d.dy / 2 - showTypeOffset : d.dy / 2)
            .attr("dy", ".35em")
            .attr("text-anchor", "middle")
            .text(d => d.name)
            .each(function (d) {
                self.wrap(this, d.dx);
            });

        cell.filter(d => showTypePredicate(d))
            .append("svg:text")
            .attr("x", d => d.dx / 2)
            .attr("y", d => showTypePredicate(d) ? d.dy / 2 + showTypeOffset : d.dy / 2)
            .attr("dy", ".35em")
            .attr("text-anchor", "middle")
            .text(d => _.upperFirst(d.type))
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

    private findDatafile(d: storageReportItem): storageReportItem {
        while (d.parent.parent && d.parent.parent.name !== "/") {
            d = d.parent;
        }
        
        return d;
    }
    
    private loadDetailedReport(d: storageReportItem): JQueryPromise<detailedStorageReportItemDto> {
        if (!d.lazyLoadChildren) {
            return;
        }

        const datafile = this.findDatafile(d);
        const full = datafile !== d; // load full details only when loadDetailedReport was requested from child of data file 
        const env = datafile.parent;

        const showLoaderTimer = setTimeout(() => {
            this.showLoader(true);
        }, 100);

        return new getEnvironmentStorageReportCommand(this.db, this.location, env.name, _.capitalize(env.type), full)
            .execute()
            .done((envReport) => {
                this.mapDetailedReport(envReport.Report, datafile);
            })
            .always(() => {
                if (this.showLoader()) {
                    this.showLoader(false);
                } else {
                    clearTimeout(showLoaderTimer);
                }
            });
    }
    
    private getNodePath(d: storageReportItem): string[] {
        const result: string[] = [];
        
        do {
            result.push(d.name);
            d = d.parent;
        } while (d.name !== "/");
        
        return result.reverse();
    }
    
    private findNodeByPath(path: string[]): storageReportItem {
        let target = this.root;

        for (const item of path) {
            target = target.internalChildren.find(x => x.name === item);

            if (!target) {
                return null;
            }
        }
        
        return target;
    }

    onClick(d: storageReportItem, goingIn: boolean) {
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

        this.updateTooltips();
        
        if (d3.event) {
            (d3.event as any).stopPropagation();
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

    private onMouseOver(d: storageReportItem) {
        this.tooltip.transition()
            .duration(200)
            .style("opacity", 1);
        let html = "<div class='tooltip-li'>Name: <div class='value'>" + d.name + "</div></div>";
        if (d.showType) {
            html += "<div class='tooltip-li'>Type: <div class='value'>" + _.upperFirst(d.type) + "</div></div>";
        }
        if (this.shouldDisplayNumberOfEntries(d)) {
            html += "<div class='tooltip-li'>Entries: <div class='value'>" + d.numberOfEntries.toLocaleString() + "</div></div>";
        }
        html += "<div class='tooltip-li'>Size: <div class='value'>" + generalUtils.formatBytesToSize(d.size) + "</div></div>";

        this.tooltip.html(html);
        this.onMouseMove();
    }

    private shouldDisplayNumberOfEntries(d: storageReportItem) {
        return d.type === "tree" || d.type === "table";
    }

    private onMouseOut() {
        this.tooltip.transition()
            .duration(500)
            .style("opacity", 0);
    }

    dataSizeFormatted(item: storageReportItem) {
        if (!item.isStorageEnvironment()) {
            return "n/a";
        }
        
        const data = item.internalChildren.find(x => x.type === "data");
        const journals = item.internalChildren.find(x => x.type === "journals");
        return generalUtils.formatBytesToSize(data.size + journals.size);
    }
    
    tempSizeFormatted(item: storageReportItem) {
        if (!item.isStorageEnvironment()) {
            return "n/a";
        }

        const tempFiles = item.internalChildren.find(x => x.type === "tempFiles");
        return generalUtils.formatBytesToSize(tempFiles.size);
    }

    compactDatabase() {
        router.navigate(appUrl.forDatabases("compact", this.db.name, this.location?.shardNumber));
    }
}

export = storageReport;
