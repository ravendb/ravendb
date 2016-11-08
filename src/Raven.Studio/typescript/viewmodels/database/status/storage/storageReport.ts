import viewModelBase = require("viewmodels/viewModelBase");
import getStorageReportCommand = require("commands/database/debug/getStorageReportCommand");
import generalUtils = require("common/generalUtils");
import app = require("durandal/app");

type treeMapItem = {
    name: string;
    type: string;
    internalChildren: treeMapItem[];
    size?: number;
    x?: number;
    y?: number;
    dx?: number;
    dy?: number;
    parent?: treeMapItem;
    w?: number; // used for storing text width
}

class storageReport extends viewModelBase {

    private rawData = [] as storageReportItem[];

    private currentPath: KnockoutComputed<Array<treeMapItem>>;

    private totalSize = ko.observable<number>();

    private x: d3.scale.Linear<number, number>;
    private y: d3.scale.Linear<number, number>;
    private color = d3.scale.category20();
    private root: treeMapItem;
    private node = ko.observable<treeMapItem>();
    private treemap: d3.layout.Treemap<any>;
    private svg: d3.Selection<any>;
    private g: d3.Selection<any>;
    private tooltip: d3.Selection<any>;

    private w = 1200;
    private h = 500;

    private kx: number;
    private ky: number;

    activate(args: any) {
        super.activate(args);

        this.initObservables();

        return new getStorageReportCommand(this.activeDatabase())
            .execute()
            .done(result => {
                this.rawData = result;
            });
    }

    private initObservables() {
        this.currentPath = ko.pureComputed(() => {
            const node = this.node();

            const items = [] as Array<treeMapItem>;

            let currentItem = node;
            while (currentItem) {
                items.unshift(currentItem);
                currentItem = currentItem.parent;
            }

            return items;
        });
    }

    private processData() {
        const data = this.rawData;

        this.root = {
            name: "root",
            internalChildren: data.map(x => this.mapReport(x)),
            type: "root"
        } as treeMapItem;

        this.sortBySize(this.root);

        this.totalSize(this.root.internalChildren.reduce((p, c) => p + c.size, 0));

        this.node(this.root);
    }

    private sortBySize(node: treeMapItem) {
        if (node.internalChildren && node.internalChildren.length) {
            node.internalChildren.forEach(x => this.sortBySize(x));

            node.internalChildren.sort((a, b) => d3.descending(a.size, b.size));
        }
    }

    private mapReport(reportItem: storageReportItem): treeMapItem {
        const dataFile = this.mapDataFile(reportItem.Report);
        const journals = this.mapJournals(reportItem.Report)
        return {
            name: reportItem.Type + ": " + reportItem.Name,
            size: dataFile.size + journals.size,
            type: reportItem.Type.toLowerCase(),
            internalChildren: [
                dataFile,
                journals
            ]
        }
    }

    private mapDataFile(report: Voron.Debugging.StorageReport): treeMapItem {
        const dataFile = report.DataFile;

        const tables = this.mapTables(report.Tables);
        const trees = this.mapTrees(report.Trees, "Trees");
        const freeSpace = {
            name: "Free",
            type: "free",
            size: dataFile.FreeSpaceInBytes,
            internalChildren: []
        } as treeMapItem;

        const totalSize = tables.size + trees.size + freeSpace.size;

        return {
            name: "Datafile",
            type: "data",
            size: totalSize,
            internalChildren: [
                tables,
                trees,
                freeSpace
            ]
        };
    }

    private mapTables(tables: Voron.Data.Tables.TableReport[]): treeMapItem {
        const mappedTables = tables.map(x => this.mapTable(x));
        return {
            name: "Tables",
            type: "tables",
            internalChildren: mappedTables,
            size: mappedTables.reduce((p, c) => p + c.size, 0)
        }
    }

    private mapTable(table: Voron.Data.Tables.TableReport): treeMapItem {
        const structure = this.mapTrees(table.Structure, "Structure");
        const data = {
            name: "Table Data",
            type: "table_data",
            size: table.DataSizeInBytes,
            internalChildren: []
        } as treeMapItem;
        const indexes = this.mapTrees(table.Indexes, "Indexes");

        const totalSize = structure.size + table.DataSizeInBytes + indexes.size;

        return {
            name: table.Name,
            type: "table",
            size: totalSize,
            internalChildren: [
                structure,
                data,
                indexes
            ]
        } as treeMapItem;
    }

    private mapTrees(trees: Voron.Debugging.TreeReport[], name: string): treeMapItem {
        return {
            name: name,
            type: name.toLowerCase(),
            internalChildren: trees.map(x => this.mapTree(x)),
            size: trees.reduce((p, c) => p + c.AllocatedSpaceInBytes, 0)
        }
    }

    private mapTree(tree: Voron.Debugging.TreeReport): treeMapItem {
        return {
            name: tree.Name,
            type: "tree",
            size: tree.AllocatedSpaceInBytes,
            internalChildren: []
        }
    }

    private mapJournals(report: Voron.Debugging.StorageReport): treeMapItem {
        const journals = report.Journals;

        const mappedJournals = journals.map(journal => {
            return {
                name: "Journal #" + journal.Number,
                type: "journal",
                size: journal.AllocatedSpaceInBytes,
                internalChildren: []
            } as treeMapItem;
        });

        return {
            name: "journals",
            type: "journals",
            internalChildren: mappedJournals,
            size: mappedJournals.reduce((p, c) => p + c.size, 0)
        }
    }

    private transform(d: any) {
        return "translate(8," + d.dx * this.ky / 2 + ")";
    }

    private initGraph() {
        this.color.domain(["documents", "index", "subscriptions", "configuration", "free", "data", "tables", "table_data", "table", "tree", "journal", "journals", "indexes", "structure"]);

        this.x = d3.scale.linear().range([0, this.w]);
        this.y = d3.scale.linear().range([0, this.h]);

        this.svg = d3.select("#storage-report-container").append("div")
            .attr("class", "chart")
            .style("width", this.w + "px")
            .style("height", this.h + "px")
            .append("svg:svg")
            .attr("width", this.w)
            .attr("height", this.h)
            .attr("transform", "translate(.5,.5)");

        this.addHashing();
    }

    private addHashing() {
        const defs = this.svg.append('defs');
        const g = defs.append("pattern")
            .attr('id', 'hash')
            .attr('patternUnits', 'userSpaceOnUse')
            .attr('width', '10')
            .attr('height', '10')
            .append("g").style("fill", "none")
            .style("stroke", "grey")
            .style("stroke-width", 1);
        g.append("path").attr("d", "M0,0 l10,10");
        g.append("path").attr("d", "M10,0 l-10,10");
    }

    private getChildren(node: any, depth: number) {
        return depth === 0 ? node.internalChildren : [];
    }

    private draw() {
        this.treemap = d3.layout.treemap<any>()
            .children((n, depth) => this.getChildren(n, depth))
            .value(d => d.size)
            .size([this.w, this.h]);

        this.tooltip = d3.select(".tooltip");

        const nodes = this.treemap.nodes(this.node())
            .filter(n => !n.children);

        const self = this;

        this.svg.selectAll("g.cell").remove();

        const cell = this.svg.selectAll("g.cell")
            .data(nodes)
            .enter().append("svg:g")
            .attr("class", d => "cell " + d.type)
            .attr("transform", d => "translate(" + d.x + "," + d.y + ")")
            .on("click", d => this.onClick(d))
            .on("mouseover", d => this.onMouseOver(d))
            .on("mouseout", d => this.onMouseOut(d))
            .on("mousemove", d => this.onMouseMove(d));

        cell.append("svg:rect")
            .attr("width", d => d.dx - 1)
            .attr("height", d => d.dy - 1)
            .attr("fill", d => d.type === "free" ? "url(#hash)" : this.color(d.type));

        cell.append("svg:text")
            .attr("x", d => d.dx / 2)
            .attr("y", d => d.dy / 2)
            .attr("dy", ".35em")
            .attr("text-anchor", "middle")
            .text(d => d.name)
            .each(function (d) {
                self.wrap(this, d.dx);
            });
    }

    wrap($self: any, width: number) {
        var self = d3.select($self),
            textLength = (self.node() as any).getComputedTextLength(),
            text = self.text();
        while (textLength > (width - 6) && text.length > 0) {
            text = text.slice(0, -1);
            self.text(text + '...');
            textLength = (self.node() as any).getComputedTextLength();
        }
    } 

    private onClick(d: treeMapItem) {
        if (!d.internalChildren || !d.internalChildren.length) {
            return;
        }
        this.node(d);
        this.draw();

        (d3.event as any).stopPropagation();
    }

    //TODO: use d3-tip library
    private onMouseMove(d: treeMapItem) {
        const [x, y] = d3.mouse(this.svg.node());
        this.tooltip
            .style("left", x + "px")
            .style("top", (y - 15) + "px");
    }

    private onMouseOver(d: treeMapItem) {
        this.tooltip.transition()
            .duration(200)
            .style("opacity", .9);
        this.tooltip.html("Name: " + d.name + " <br /> Size: " + generalUtils.formatBytesToSize(d.size));
        this.onMouseMove(d);
    }

    private onMouseOut(d: treeMapItem) {
        this.tooltip.transition()
            .duration(500)
            .style("opacity", 0);	
    }

    compositionComplete() {
        super.compositionComplete();
        this.processData();
        this.initGraph();
        this.draw();
    }

    formatSize(size: number) {
        return generalUtils.formatBytesToSize(size);
    }
}

export = storageReport;    
