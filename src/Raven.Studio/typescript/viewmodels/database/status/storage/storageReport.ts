import viewModelBase = require("viewmodels/viewModelBase");
import getStorageReportCommand = require("commands/database/debug/getStorageReportCommand");

type treeMapItem = {
    name: string;
    children?: treeMapItem[];
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

    private currentPath = ko.observable<string>(); //TODO: replace with array of items

    private processData() {
        const data = this.rawData;

        this.node = this.root = {
            name: "root",
            children: data.map(x => this.mapReport(x))
        } as treeMapItem;
    }

    private mapReport(reportItem: storageReportItem): treeMapItem {
        return {
            name: reportItem.Type + ": " + reportItem.Name,
            children: [
                this.mapDataFile(reportItem.Report),
                this.mapJournals(reportItem.Report)
            ]
        }
    }

    private mapDataFile(report: Voron.Debugging.StorageReport): treeMapItem {
        const dataFile = report.DataFile;

        const tables = this.mapTables(report.Tables);
        const trees = this.mapTrees(report.Trees, "Trees");
        const freeSpace = {
            name: "Free",
            size: dataFile.FreeSpaceInBytes
        } as treeMapItem;

        return {
            name: "datafile",
            children: [
                tables,
                trees,
                freeSpace
            ]
        };
    }

    private mapTables(tables: Voron.Data.Tables.TableReport[]): treeMapItem {
        return {
            name: "Tables",
            children: tables.map(x => this.mapTable(x))
        }
    }

    private mapTable(table: Voron.Data.Tables.TableReport): treeMapItem {
        const structure = this.mapTrees(table.Structure, "Structure");
        const data = {
            name: "Data",
            size: table.DataSizeInBytes
        } as treeMapItem;
        const indexes = this.mapTrees(table.Indexes, "Indexes");

        return {
            name: table.Name,
            children: [
                structure,
                data,
                indexes
            ]
        } as treeMapItem;
    }

    private mapTrees(trees: Voron.Debugging.TreeReport[], name: string): treeMapItem {
        return {
            name: "Trees",
            children: trees.map(x => this.mapTree(x))
        }
    }

    private mapTree(tree: Voron.Debugging.TreeReport): treeMapItem {
        return {
            name: tree.Name,
            size: tree.AllocatedSpaceInBytes
        }
    }

    private mapJournals(report: Voron.Debugging.StorageReport): treeMapItem {
        const journals = report.Journals;

        const mappedJournals = journals.map(journal => {
            return {
                name: "Journal #" + journal.Number,
                size: journal.AllocatedSpaceInBytes
            } as treeMapItem;
        });

        return {
            name: "journals",
            children: mappedJournals
        }
    }

    private x: d3.scale.Linear<number, number>;
    private y: d3.scale.Linear<number, number>;
    private color = d3.scale.category20c();
    private root: treeMapItem;
    private node: treeMapItem;
    private partition: d3.layout.Partition<any>;
    private svg: d3.Selection<any>;
    private g: d3.Selection<any>;

    private w = 1280 - 80;
    private h = 800 - 180;

    private kx: number;
    private ky: number;

    private transform(d: any) {
        return "translate(8," + d.dx * this.ky / 2 + ")";
    }

    private draw() {
        this.x = d3.scale.linear().range([0, this.w]);
        this.y = d3.scale.linear().range([0, this.h]);

        this.svg = d3.select("#storage-report-container").append("div")
            .attr("class", "chart")
            .style("width", this.w + "px")
            .style("height", this.h + "px")
            .append("svg:svg")
            .attr("width", this.w)
            .attr("height", this.h);

        this.partition = d3.layout.partition<any>()
            .value(d => d.size);

        this.g = this.svg.selectAll("g")
            .data(this.partition.nodes(this.root))
            .enter()
            .append("svg:g")
            .attr("transform", d => "translate(" + this.x(d.y) + "," + this.y(d.x) + ")")
            .on('mouseover', d => this.onEnter(d))
            .on('mouseout', d => this.onExit(d))
            .on("click", d => this.onClick(d));

        this.kx = this.w / this.root.dx;
        this.ky = this.h / 1;

        this.g.append("svg:rect")
            .attr("width", this.root.dy * this.kx)
            .attr("height", d => d.dx * this.ky)
            .attr("class", d => d.children ? "parent" : "child");

        this.g.append("svg:text")
            .attr("transform", d => this.transform(d))
            .attr("dy", ".35em")
            .style("opacity", d => d.dx * this.ky > 12 ? 1 : 0)
            .text(d => d.name);
    }

    private onEnter(d: treeMapItem) {
        const items = [] as Array<string>;

        let currentItem = d;
        while (currentItem) {
            items.unshift(currentItem.name);
            currentItem = currentItem.parent;
        }

        this.currentPath(items.join(" > "));
    }

    private onExit(d: treeMapItem) {
        this.onEnter(this.node);
    }

    private onClick(d: any) {
        this.node = d;

        this.kx = (d.y ? this.w - 40 : this.w) / (1 - d.y);
        this.ky = this.h / d.dx;
        this.x.domain([d.y, 1]).range([d.y ? 40 : 0, this.w]);
        this.y.domain([d.x, d.x + d.dx]);

        const t = this.g.transition()
            .duration(750)
            .attr("transform", d => "translate(" + this.x(d.y) + "," + this.y(d.x) + ")");

        t.select("rect")
            .attr("width", d.dy * this.kx)
            .attr("height", d => d.dx * this.ky);

        t.select("text")
            .attr("transform", d => this.transform(d))
            .style("opacity", d => d.dx * this.ky > 12 ? 1 : 0);

        (d3.event as any).stopPropagation();
    }

    activate(args: any) {
        super.activate(args);

        new getStorageReportCommand(this.activeDatabase())
            .execute()
            .done(result => {
                this.rawData = result;
            });
    }

    compositionComplete() {
        super.compositionComplete();
        this.processData();
        this.draw();
    }
}

export = storageReport;    
