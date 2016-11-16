import viewModelBase = require("viewmodels/viewModelBase");
import getStorageReportCommand = require("commands/database/debug/getStorageReportCommand");
import generalUtils = require("common/generalUtils");
import app = require("durandal/app");
import storageReportItem = require("models/database/status/storageReportItem");

class storageReport extends viewModelBase {

    private rawData = [] as storageReportItemDto[];

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

    showPagesColumn: KnockoutObservable<boolean>;
    showEntriesColumn: KnockoutObservable<boolean>;

    activate(args: any) {
        super.activate(args);

        this.initObservables();

        return new getStorageReportCommand(this.activeDatabase())
            .execute()
            .done(result => {
                this.rawData = result;
            });
    }

    compositionComplete() {
        super.compositionComplete();
        this.processData();
        this.initGraph();
        this.draw();
    }

    private initObservables() {
        this.currentPath = ko.pureComputed(() => {
            const node = this.node();

            const items = [] as Array<storageReportItem>;

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
    }

    private processData() {
        const data = this.rawData;

        const mappedData = data.map(x => this.mapReport(x));
        const totalSize = mappedData.reduce((p, c) => p + c.size, 0);
        const item = new storageReportItem("root", "root", false, totalSize, mappedData);

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

        return new storageReportItem(reportItem.Name,
            reportItem.Type.toLowerCase(),
            storageReport.showDisplayReportType(reportItem.Type),
            dataFile.size + journals.size,
            [dataFile, journals]);
    }

    private static showDisplayReportType(reportType: string): boolean {
        return reportType !== "Configuration" && reportType !== "Subscriptions";
    }

    private mapDataFile(report: Voron.Debugging.StorageReport): storageReportItem {
        const dataFile = report.DataFile;

        const tables = this.mapTables(report.Tables);
        const trees = this.mapTrees(report.Trees, "Trees");

        const freeSpace = new storageReportItem("Free", "free", false, dataFile.FreeSpaceInBytes, []);

        const totalSize = tables.size + trees.size + freeSpace.size;

        return new storageReportItem("Datafile", "data", false, totalSize, [tables, trees, freeSpace]);
    }

    private mapTables(tables: Voron.Data.Tables.TableReport[]): storageReportItem {
        const mappedTables = tables.map(x => this.mapTable(x));

        return new storageReportItem("Tables", "tables", false, mappedTables.reduce((p, c) => p + c.size, 0), mappedTables);
    }

    private mapTable(table: Voron.Data.Tables.TableReport): storageReportItem {
        const structure = this.mapTrees(table.Structure, "Structure");

        const data = new storageReportItem("Table Data", "table_data", false, table.DataSizeInBytes, []);
        const indexes = this.mapTrees(table.Indexes, "Indexes");

        const totalSize = structure.size + table.DataSizeInBytes + indexes.size;

        const tableItem = new storageReportItem(table.Name, "table", true, totalSize, [
            structure,
            data,
            indexes
        ]);

        tableItem.numberOfEntries = table.NumberOfEntries;

        return tableItem;
    }

    private mapTrees(trees: Voron.Debugging.TreeReport[], name: string): storageReportItem {
        return new storageReportItem(name, name.toLowerCase(), false, trees.reduce((p, c) => p + c.AllocatedSpaceInBytes, 0), trees.map(x => this.mapTree(x)));
    }

    private mapTree(tree: Voron.Debugging.TreeReport): storageReportItem {
        const item = new storageReportItem(tree.Name, "tree", true, tree.AllocatedSpaceInBytes, []);
        item.pageCount = tree.PageCount;
        item.numberOfEntries = tree.NumberOfEntries;
        return item;
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

    private initGraph() {
        this.detectContainerSize();
        this.x = d3.scale.linear().range([0, this.w]);
        this.y = d3.scale.linear().range([0, this.h]);

        this.svg = d3.select("#storage-report-container .chart")
            .append("svg:svg")
            .attr("width", this.w)
            .attr("height", this.h)
            .attr("transform", "translate(.5,.5)");

        this.addHashing();
    }

    private detectContainerSize() {
        const $chartNode = $("#storage-report-container .chart")
        this.w = $chartNode.width();
        this.h = $chartNode.height();
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

        this.tooltip = d3.select(".chart-tooltip");

        const nodes = this.treemap.nodes(this.node())
            .filter(n => !n.children);

        const self = this;
        const showTypeOffset = 7;
        const showTypePredicate = (d: storageReportItem) => d.showType && d.dy > 14 && d.dx > 14;

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
            .attr("height", d => d.dy - 1);

        cell.append("svg:text")
            .filter(d => d.dx > 14 && d.dy > 6)
            .attr("x", d => d.dx / 2)
            .attr("y", d => showTypePredicate(d) ? d.dy/2 - showTypeOffset : d.dy / 2)
            .attr("dy", ".35em")
            .attr("text-anchor", "middle")
            .text(d => d.name)
            .each(function (d) {
                self.wrap(this, d.dx);
            });

        cell.filter(d => showTypePredicate(d))
            .append("svg:text")
            .attr("x", d => d.dx / 2)
            .attr("y", d => showTypePredicate(d) ? d.dy/2 + showTypeOffset : d.dy / 2)
            .attr("dy", ".35em")
            .attr("text-anchor", "middle")
            .text(d => d.type.capitalizeFirstLetter())
            .each(function (d) {
                self.wrap(this, d.dx);
            });
    }

    wrap($self: any, width: number) {
        let self = d3.select($self),
            textLength = (self.node() as any).getComputedTextLength(),
            text = self.text();
        while (textLength > (width - 6) && text.length > 0) {
            text = text.slice(0, -1);
            self.text(text + '...');
            textLength = (self.node() as any).getComputedTextLength();
        }
    } 

    private onClick(d: storageReportItem) {
        if (!d.internalChildren || !d.internalChildren.length) {
            return;
        }
        this.node(d);
        this.draw();

        if (d3.event) {
            (d3.event as any).stopPropagation();
        }
    }

    private onMouseMove(d: storageReportItem) {
        const [x, y] = d3.mouse(this.svg.node());
        let offset = d.showType ? 38 : 15;
        if (this.shouldDisplayNumberOfEntires(d)) {
            offset += 23;
        }
        this.tooltip
            .style("left", x + "px")
            .style("top", (y - offset) + "px");
    }

    private onMouseOver(d: storageReportItem) {
        this.tooltip.transition()
            .duration(200)
            .style("opacity", 1);
        let html = "Name: " + d.name;
        if (d.showType) {
            html += "<br />Type: <strong>" + d.type.capitalizeFirstLetter() + "</strong>";
        }
        if (this.shouldDisplayNumberOfEntires(d)) {
            html += "<br />Entries: <strong>" + d.numberOfEntries + "</strong>";
        }
        html += " <br /> <span class='size'>Size: <strong>" + generalUtils.formatBytesToSize(d.size) + "</strong></span>";

        this.tooltip.html(html);
        this.onMouseMove(d);
    }

    private shouldDisplayNumberOfEntires(d: storageReportItem) {
        return d.type === "tree" || d.type === "table";
    }

    private onMouseOut(d: storageReportItem) {
        this.tooltip.transition()
            .duration(500)
            .style("opacity", 0);	
    }

   
}

export = storageReport;    
