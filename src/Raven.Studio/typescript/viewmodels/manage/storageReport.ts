import viewModelBase = require("viewmodels/viewModelBase");
import getSystemStorageReportCommand = require("commands/resources/getSystemStorageReportCommand");
import generalUtils = require("common/generalUtils");
import { select, Selection, pointer } from "d3-selection";
import { descending } from "d3-array";
import { treemap, hierarchy, HierarchyNode, HierarchyRectangularNode } from "d3-hierarchy";
import "d3-transition";

type positionAndSizes = {
    dx: number,
    dy: number,
    x: number,
    y: number
}

export class storageReport extends viewModelBase {

    view = require("views/manage/storageReport.html");

    static readonly animationLength = 200;

    currentPath: KnockoutObservable<serverStorageReportItem[]>;
    private rootData: serverStorageReportItem;
    private rootHierarchy: HierarchyNode<serverStorageReportItem>;
    private previousTreeMap: HierarchyRectangularNode<serverStorageReportItem>;
    private node = ko.observable<serverStorageReportItem>();
    private svg: Selection<any, unknown, HTMLElement, any>;
    private tooltip: Selection<null, unknown, HTMLElement, any>;

    private w: number;
    private h: number;

    private transitioning = false;

    showPagesColumn: KnockoutObservable<boolean>;
    showEntriesColumn: KnockoutObservable<boolean>;
    showTempFiles: KnockoutObservable<boolean>;

    constructor() {
        super();
        this.bindToCurrentInstance("onClick");
    }

    activate(args: any) {
        super.activate(args);

        this.initObservables();

        return new getSystemStorageReportCommand()
            .execute()
            .done(result => {
                this.processData(result);
            });
    }

    compositionComplete() {
        super.compositionComplete();
        this.initGraph();
        this.draw(undefined);
    }

    private initObservables() {
        this.currentPath = ko.pureComputed(() => {
            const node = this.node();
            const treeToLayout = this.rootHierarchy.find(x => x.data === node);
            return treeToLayout.ancestors().map(x => x.data).reverse();
        });
        
        this.showEntriesColumn = ko.pureComputed(() => {
            const node = this.node();
            if (!node) {
                return false;
            }
            return !!node.internalChildren.find(x => x.type === "table" || x.type === "tree");
        });

        this.showPagesColumn = ko.pureComputed(() => {
            const node = this.node();
            if (!node) {
                return false;
            }
            return !!node.internalChildren.find(x => x.type === "tree");
        });
        
        this.showTempFiles = ko.pureComputed(() => {
            return this.node() === this.rootData;
        });
    }

    private processData(data: detailedSystemStorageReportItemDto) {
        const mappedReport = mapReport(data);
        sortBySize(mappedReport);
        this.rootData = mappedReport;
        this.rootHierarchy = createHierarchyWithValues(mappedReport);
        this.node(this.rootData);
    }
    
    private initGraph() {
        this.detectContainerSize();
        
        this.svg = select("#storage-report-container .chart")
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

    private draw(goingIn: boolean) {
        const levelDown = goingIn === true;
        const levelUp = goingIn === false;
        
        const node = this.node();

        const oldNode = this.previousTreeMap?.find(x => x.data === node);
        
        const oldLocation: positionAndSizes = { 
            dx: oldNode ? oldNode.x1 - oldNode.x0 : this.w,
            dy: oldNode ? oldNode.y1 - oldNode.y0 : this.h,
            x: oldNode ? oldNode.x0 : 0,
            y: oldNode ? oldNode.y0 : 0,
        };
        
        // construct fake hierarchy based on current node and it's children only - as we draw one level at the time
        const flatHierarchy = createHierarchyWithValues(node, true);
        const currentRoot = treemap<serverStorageReportItem>()
            .size([this.w, this.h])
                (flatHierarchy);
        
        this.previousTreeMap = currentRoot;
        this.tooltip = select(".chart-tooltip");
        
        const nodes = currentRoot.children;

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

    private animateZoomIn(nodes: HierarchyRectangularNode<serverStorageReportItem>[], oldLocation: positionAndSizes) {
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
            .on("end", () => {
                const newCells = this.drawNewTreeMap(nodes, newGroup);
                newCells
                    .style('opacity', 0)
                    .transition()
                    .duration(storageReport.animationLength)
                    .style('opacity', 1)
                    .on("end", () => {
                        oldContainer.remove();
                        this.transitioning = false;
                    });
            });
    }

    private animateZoomOut(nodes: HierarchyRectangularNode<serverStorageReportItem>[]) {
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
            .on("end", () => {
                oldContainer.remove();
                this.transitioning = false;
            });
    }

    private drawNewTreeMap(nodes: HierarchyRectangularNode<serverStorageReportItem>[], container: Selection<any, any, HTMLElement, any>) {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        const showTypeOffset = 7;
        const showTypePredicate = (d: HierarchyRectangularNode<serverStorageReportItem>) => d.data.showType && (d.y1 - d.y0) > 22 && (d.x1 - d.x0) > 20;

        const cell = container.selectAll("g.cell-no-such") // we always select non-existing nodes to draw from scratch - we don't update elements here
            .data(nodes)
            .enter()
            .append("svg:g")
            .attr("class", d => "cell " + d.data.type)
            .attr("transform", d => "translate(" + d.x0 + "," + d.y0 + ")")
            .on("click", (event: PointerEvent, d) => this.onClick(event, d.data, true))
            .on("mouseover", (event, data) => this.onMouseOver(event, data.data))
            .on("mouseout", () => this.onMouseOut())
            .on("mousemove", (e) => this.onMouseMove(e));

        const rectangles = cell.append("svg:rect")
            .attr("width", d => Math.max(0, (d.x1 - d.x0) - 1))
            .attr("height", d => Math.max(0, (d.y1 - d.y0) - 1));

        rectangles
            .filter(x => x.data.hasChildren())
            .style('cursor', 'pointer');
        
        cell.append("svg:text")
            .filter(d => (d.x1 - d.x0) > 20 && (d.y1 - d.y0) > 8)
            .attr("x", d => (d.x1 - d.x0) / 2)
            .attr("y", d => showTypePredicate(d) ? (d.y1 - d.y0) / 2 - showTypeOffset : (d.y1 - d.y0) / 2)
            .attr("dy", ".35em")
            .attr("text-anchor", "middle")
            .text(d => d.data.name)
            .each(function (d) {
                self.wrap(this, (d.x1 - d.x0));
            });

        cell.filter(d => showTypePredicate(d))
            .append("svg:text")
            .attr("x", d => (d.x1 - d.x0) / 2)
            .attr("y", d => showTypePredicate(d) ? (d.y1 - d.y0) / 2 + showTypeOffset : (d.y1 - d.y0) / 2)
            .attr("dy", ".35em")
            .attr("text-anchor", "middle")
            .text(d => _.upperFirst(d.data.type))
            .each(function (d) {
                self.wrap(this, (d.x1 - d.x0));
            });

        return cell;
    }

    wrap($self: any, width: number) {
        const self = select($self);
        let textLength = (self.node() as any).getComputedTextLength();
        let text = self.text();
        while (textLength > (width - 6) && text.length > 0) {
            text = text.slice(0, -1);
            self.text(text + '...');
            textLength = (self.node() as any).getComputedTextLength();
        }
    } 

    onClick(event: PointerEvent, d: serverStorageReportItem, goingIn: boolean) {
        if (this.transitioning || this.node() === d) {
            return;
        }

        if (!d.internalChildren || !d.internalChildren.length) {
            // it is a leaf node - prevent click
            return;
        }

        this.node(d);
        this.draw(goingIn);

        this.updateTooltips();
        
        if (event) {
            event.stopPropagation();
        }
    }
    
    private updateTooltips() {
        $('#storage-report [data-toggle="tooltip"]').tooltip();
    }

    private onMouseMove(e: any) {
        // eslint-disable-next-line prefer-const
        let [x, y] = pointer(e, this.svg.node());

        const tooltipWidth = $(".chart-tooltip").width() + 20;

        x = Math.min(x, Math.max(this.w - tooltipWidth, 0));

        this.tooltip
            .style("left", (x + 10) + "px")
            .style("top", (y + 10) + "px");
    }

    private onMouseOver(event: any, d: serverStorageReportItem) {
        this.tooltip.transition()
            .duration(200)
            .style("opacity", 1);
        
        let html = "<span class='name'>Name: " + d.name + "</span>";
        if (d.showType) {
            html += "<span>Type: <strong>" + _.upperFirst(d.type) + "</strong></span>";
        }
        if (this.shouldDisplayNumberOfEntries(d)) {
            html += "<span>Entries: <strong>" + d.numberOfEntries.toLocaleString() + "</strong></span>";
        }
        html += "<span class='size'>Size: <strong>" + generalUtils.formatBytesToSize(d.size) + "</strong></span>";

        this.tooltip.html(html);
        this.onMouseMove(event);
    }

    private shouldDisplayNumberOfEntries(d: serverStorageReportItem) {
        return d.type === "tree" || d.type === "table";
    }

    private onMouseOut() {
        this.tooltip.transition()
            .duration(500)
            .style("opacity", 0);
    }
}

class serverStorageReportItem {
    name: string;
    type: string;
    internalChildren: serverStorageReportItem[];
    size?: number;
    length?: number;
    pageCount: number = null;
    showType: boolean;
    w?: number; // used for storing text width
    numberOfEntries: number = null;
    customSizeProvider: (header: boolean) => string;
    isGrouped: boolean;

    recyclableJournal = false;

    constructor(name: string, type: string, showType: boolean, size: number, internalChildren: serverStorageReportItem[] = null, isGrouped = false) {
        this.name = name;
        this.type = type;
        this.showType = showType;
        this.size = size;
        this.internalChildren = internalChildren;
        this.isGrouped = isGrouped;
    }

    formatSize(header: boolean) {
        return this.customSizeProvider ? this.customSizeProvider(header) : generalUtils.formatBytesToSize(this.size);
    }

    formatPercentage(parentSize: number) {
        return (this.size * 100 / parentSize).toFixed(2) + '%';
    }

    hasChildren(): boolean {
        return this.internalChildren && this.internalChildren.length > 0;
    }
}


function createHierarchyWithValues(node: serverStorageReportItem, flat = false): HierarchyNode<serverStorageReportItem> {
    const childrenExtractor = flat
        ? (d: serverStorageReportItem) => d === node ? d.internalChildren : []
        : (d: serverStorageReportItem) => d.internalChildren;

    return hierarchy<serverStorageReportItem>(node, childrenExtractor)
        .eachBefore(d => {
            // we don't use sum here, as the values are already summed-up - instead update readonly property 'value'
            (d.value as number) = d.data.size;
        });
}

function mapReport(reportItem: detailedSystemStorageReportItemDto): serverStorageReportItem {
    const dataFile = mapDataFile(reportItem.Report);
    const journals = mapJournals(reportItem.Report);
    const tempFiles = mapTempFiles(reportItem.Report);

    return new serverStorageReportItem(reportItem.Environment,
        reportItem.Type.toLowerCase(),
        true,
        dataFile.size + journals.size + tempFiles.size,
        [dataFile, journals, tempFiles]);
}

function mapDataFile(report: Voron.Debugging.DetailedStorageReport): serverStorageReportItem {
    const dataFile = report.DataFile;

    const d = new serverStorageReportItem("Datafile", "data", false, dataFile.AllocatedSpaceInBytes);
    const tables = mapTables(report.Tables);
    const trees = mapTrees(report.Trees, "Trees");
    const freeSpace = new serverStorageReportItem("Free", "free", false, report.DataFile.FreeSpaceInBytes, []);
    const preallocatedBuffers = mapPreAllocatedBuffers(report.PreAllocatedBuffers);

    d.internalChildren = [tables, trees, freeSpace, preallocatedBuffers];

    return d;
}

function mapPreAllocatedBuffers(buffersReport: Voron.Debugging.PreAllocatedBuffersReport): serverStorageReportItem {
    const allocationTree = mapTree(buffersReport.AllocationTree);
    const buffersSpace = new serverStorageReportItem("Pre Allocated Buffers Space", "reserved", false, buffersReport.PreAllocatedBuffersSpaceInBytes);
    buffersSpace.pageCount = buffersReport.NumberOfPreAllocatedPages;

    const preAllocatedBuffers = new serverStorageReportItem("Pre Allocated Buffers", "reserved", false, buffersReport.AllocatedSpaceInBytes, [allocationTree, buffersSpace]);
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

function mapTables(tables: Voron.Data.Tables.TableReport[]): serverStorageReportItem {
    const mappedTables = tables.map(x => mapTable(x));

    return new serverStorageReportItem("Tables", "tables", false, mappedTables.reduce((p, c) => p + c.size, 0), mappedTables);
}

function mapTable(table: Voron.Data.Tables.TableReport): serverStorageReportItem {
    const structure = mapTrees(table.Structure, "Structure");

    const data = new serverStorageReportItem("Table Data", "table_data", false, table.DataSizeInBytes, []);
    const indexes = mapTrees(table.Indexes, "Indexes");

    const preallocatedBuffers = mapPreAllocatedBuffers(table.PreAllocatedBuffers);

    const totalSize = table.AllocatedSpaceInBytes;

    const tableItem = new serverStorageReportItem(table.Name, "table", true, totalSize, [
        structure,
        data,
        indexes,
        preallocatedBuffers
    ]);

    tableItem.numberOfEntries = table.NumberOfEntries;

    return tableItem;
}

function mapTrees(trees: Voron.Debugging.TreeReport[], name: string): serverStorageReportItem {
    return new serverStorageReportItem(name, name.toLowerCase(), false, 
        trees.reduce((p, c) => p + c.AllocatedSpaceInBytes, 0), trees.map(x => mapTree(x)));
}

function mapTree(tree: Voron.Debugging.TreeReport): serverStorageReportItem {
    const children = (tree.Streams && tree.Streams.Streams) ? tree.Streams.Streams.map(x => mapStream(x)) : [];
    const item = new serverStorageReportItem(tree.Name, "tree", true, tree.AllocatedSpaceInBytes, children);
    item.pageCount = tree.PageCount;
    item.numberOfEntries = tree.NumberOfEntries;
    return item;
}

function mapStream(stream: Voron.Debugging.StreamDetails): serverStorageReportItem {
    const item = new serverStorageReportItem(stream.Name, "stream", false, stream.AllocatedSpaceInBytes, []);

    item.customSizeProvider = (header: boolean) => {
        const allocatedSizeFormatted = generalUtils.formatBytesToSize(stream.AllocatedSpaceInBytes);
        if (header) {
            return allocatedSizeFormatted;
        }
        const length = generalUtils.formatBytesToSize(stream.Length);
        return `<span title="stream length: ${length} / total allocation: ${allocatedSizeFormatted}">${length} / ${allocatedSizeFormatted}</span>`;
    }

    return item;
}

function mapJournals(report: Voron.Debugging.DetailedStorageReport): serverStorageReportItem {
    const journals = report.Journals.Journals;

    const mappedJournals = journals.map(journal =>
        new serverStorageReportItem(
            "Journal #" + journal.Number,
            "journal",
            false,
            journal.AllocatedSpaceInBytes,
            []
        ));

    return new serverStorageReportItem("Journals", "journals", false, mappedJournals.reduce((p, c) => p + c.size, 0), mappedJournals);
}

function mapTempFiles(report: Voron.Debugging.DetailedStorageReport): serverStorageReportItem {
    const tempFiles = report.TempBuffers;

    const mappedTemps = tempFiles.map(temp => {
        const item = new serverStorageReportItem(
            temp.Name,
            "temp",
            false,
            temp.AllocatedSpaceInBytes,
            []
        );

        item.recyclableJournal = temp.Type === "RecyclableJournal";

        return item;
    });

    return new serverStorageReportItem("Temporary Files", "tempFiles", false, mappedTemps.reduce((p, c) => p + c.size, 0), mappedTemps);
}

function sortBySize(node: serverStorageReportItem) {
    if (node.internalChildren && node.internalChildren.length) {
        node.internalChildren.forEach(x => sortBySize(x));
        node.internalChildren.sort((a, b) => descending(a.size, b.size));
    }
}
