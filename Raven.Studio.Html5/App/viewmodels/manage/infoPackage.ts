/// <reference path="../../../Scripts/typings/jszip/jszip.d.ts" />

import app = require("durandal/app");
import d3 = require('d3/d3');
import nv = require('nvd3');
import jszip = require('jszip/jszip');
import messagePublisher = require("common/messagePublisher");
import appUrl = require("common/appUrl");
import svgDownloader = require("common/svgDownloader");
import fileDownloader = require("common/fileDownloader");
import getInfoPackage = require('commands/database/debug/getInfoPackage');
import viewModelBase = require("viewmodels/viewModelBase");
import infoPackageImport = require("viewmodels/manage/infoPackageImport");
import shell = require("viewmodels/shell");
import eventsCollector = require("common/eventsCollector");

const enum parserState {
  pid,
  stack
}
 
class stackInfo {
    static boxPadding = 8;
    static lineHeight = 12;
    static headerSize = 16;

    static shortName(v: string) {
        var withoutArgs = v.replace(/\(.*?\)/g, '');
        if (withoutArgs.contains('+')) {
            return withoutArgs.replace(/.*\.(.*\+.*)/, '$1');
        } else {
            return withoutArgs.replace(/.*\.([^\.]+\.[^\.]+)$/, '$1');
        }
    }

    static isUserCode(line: string): boolean {
        return line.startsWith("Raven") || line.startsWith("Voron");
    }

    constructor(public ThreadIds: string[], public StackTrace: string[]) {
    }

    children: stackInfo[];
    depth: number;

    boxHeight = () => {
        return this.StackTrace.length * stackInfo.lineHeight + 2 * stackInfo.boxPadding;
    }

    stackWithShortcuts = () => {
        return this.StackTrace.map(v => {
            return {
                short: stackInfo.shortName(v),
                full: v
            }
        });
    }
}

class infoPackage extends viewModelBase {
    diagonal: any;

    node: D3.Selection = null; // nodes selection
    link: D3.Selection = null; // links selection

    private nodes: stackInfo[] = [];
    private links: D3.Layout.GraphLink[];

    private xScale: D3.Scale.LinearScale;
    private yScale: D3.Scale.LinearScale;

    private height: number;
    private width: number;

    static maxBoxWidth = 280;
    static boxVerticalPadding = 60;

    svg: D3.Selection = null;
    svgDefs: D3.Selection = null;
    graph: any = null;

    infoPackage = ko.observable<any>();
    infoPackageFilename = ko.observable<string>();
    fetchException = ko.observable<string>();
    fetchExceptionDetails = ko.observable<string>();
    showMoreDetails = ko.observable<boolean>(false);
    showLoadingIndicator = ko.observable(false);
    showMoreDetailsButton = ko.computed(() => {
        var hasDetails = !!this.fetchExceptionDetails();
        var detailsDisplayed = this.showMoreDetails();
        return hasDetails && !detailsDisplayed;
    });
    private stacksJson = ko.observable<stackInfo[]>(null);

    hasFetchException = ko.computed(() => !!this.fetchException());
    hasInfoPackage = ko.computed(() => !!this.infoPackage());
    hasStackDump = ko.computed(() => !!this.stacksJson());
    hasSaveAsPngSupport = ko.computed(() => {
        return !(navigator && navigator.msSaveBlob);
    });
    appUrls: computedAppUrls;
    adminView: KnockoutComputed<boolean>;
    isForbidden = ko.observable<boolean>();

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();
        this.adminView = ko.computed(() => {
            var activeDb = this.activeDatabase();
            var appUrls = this.appUrls;
            return (!!activeDb && activeDb.isSystem || !!appUrls && appUrls.isAreaActive('admin')());
        });
        this.isForbidden(shell.isGlobalAdmin() === false);
    }

    canActivate(args): any {
        return true;
    }

    attached() {
        super.attached();
        this.updateHelpLink('KVLC4Y');
        var self = this;
        $("#stacksContainer").resize();
        this.diagonal = d3.svg.diagonal().projection(d => [self.xScale(d.x), self.yScale(d.y)]);
    }

    detached() {
        super.detached();
        $("#stacksContainer").off('DynamicHeightSet');
        nv.tooltip.cleanup();
    }

    private splitAndCollateStacks(stacks: stackInfo[]): stackInfo[]{
        if (stacks.length === 1) {
            return stacks;
        }
        var grouped = d3.nest().key((d: stackInfo) => d.StackTrace[0]).entries(stacks);

        // for each group find common stack
        return grouped.map(kv=> {
            var sharedStacks: stackInfo[] = kv.values;
            var minDepth = d3.min(sharedStacks, s => s.StackTrace.length);

            outer:
            for (var depth = 0; depth < minDepth; depth++) {
                var currentStack = sharedStacks[0].StackTrace[depth];
                for (var i = 1; i < sharedStacks.length; i++) {
                    if (currentStack !== sharedStacks[i].StackTrace[depth]) {
                        break outer;
                    }
                }
            }

            // extract shared stack:
            var sharedStack = new stackInfo([], sharedStacks[0].StackTrace.slice(0, depth));

            // remove shared stack from all stacks and recurse
            var strippedStacks = sharedStacks.map(s => new stackInfo(s.ThreadIds, s.StackTrace.slice(depth))).filter(s => s.StackTrace.length > 0);
            sharedStack.children = this.splitAndCollateStacks(strippedStacks);
            sharedStack.ThreadIds = d3.merge(sharedStacks.map(s => s.ThreadIds));
            
            return sharedStack;
        });
    }

    private cumulativeSumWithPadding(input: any[], padding: number) {
        var currentSum = 0;
        var output = [0];
        for (var i = 0; i < input.length; i++) {
            var offset = padding + input[i];
            output.push(currentSum + offset);
            currentSum += offset;
        }
        return output;
    }

    private getTooltip(data) {
        return data.full; 
    }

    private updateGraph(roots: stackInfo[]) {
        var self = this; 

        $("#parallelStacks").empty();

        this.svgDefs = d3.select("#parallelStacks").append("defs");

        var zoom = d3.behavior.zoom().scale(0.25).scaleExtent([0.2, 1.5]).on("zoom", this.zoom.bind(self));

        this.svg = d3.select("#parallelStacks")
            .append("g")
            .call(zoom)
            .append("g");

        (<any>zoom).event(d3.select("#parallelStacks > g"));

        this.svg.append("rect")
            .attr("class", "overlay");

        this.node = this.svg.selectAll(".node");
        this.link = this.svg.selectAll(".link");

        var fakeRoot: stackInfo = new stackInfo([], []);
        fakeRoot.children = roots;

        this.graph = d3.layout.tree().nodeSize([infoPackage.maxBoxWidth + 20, 100]);
        this.nodes = this.graph.nodes(fakeRoot).filter(d => d.depth > 0);

        var maxBoxHeightOnDepth = d3.nest()
            .key(d => d.depth)
            .sortKeys(d3.ascending)
            .rollup((leaves: any[]) => d3.max(leaves, l => l.boxHeight()))
            .entries(this.nodes)
            .map(v => v.values);

        var cumulative = this.cumulativeSumWithPadding(maxBoxHeightOnDepth, infoPackage.boxVerticalPadding);

        this.height = cumulative[cumulative.length - 1];
        var extent = d3.extent(this.nodes, (node: any) => node.x);
        extent[1] += infoPackage.maxBoxWidth; 
        this.width = extent[1] - extent[0];

        d3.select(".overlay")
            .attr("width", self.width)
            .attr("height", self.height);

        var halfBoxShift = infoPackage.maxBoxWidth / 2 + 10; // little padding

        this.xScale = d3.scale.linear().domain([extent[0] - halfBoxShift, extent[1] - halfBoxShift]).range([0, this.width]);
        this.yScale = d3.scale.linear().domain([0, this.height]).range([this.height, 0]);

        var yDepthScale = d3.scale.linear().domain(d3.range(1, cumulative.length + 2, 1)).range(cumulative);

        this.links = this.graph.links(this.nodes).map(link => {
            var targetY = yDepthScale(link.target.depth);
            var linkHeight = infoPackage.boxVerticalPadding - stackInfo.headerSize;

            return {
                source: {
                    x: link.source.x,
                    y: targetY - linkHeight,
                    y0: link.source.y
                },
                target: {
                    x: link.target.x,
                    y: targetY,
                    y0: link.target.y
                }
            }
        });

        this.node = this.node.data(this.nodes);
        this.link = this.link.data(this.links);

        var enteringNodes = (<D3.UpdateSelection>this.node)
            .enter()
            .append("g") 
            .attr("transform", (d) => "translate(" + this.xScale(d.x) + "," + this.yScale(yDepthScale(d.depth)) + ")");

        enteringNodes
            .filter((d: stackInfo) => d.children && d.children.length > 0)
            .append("line")
            .attr("class", "link")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", (d: stackInfo) => -d.boxHeight() - stackInfo.headerSize)
            .attr("y2", (d: stackInfo) => -maxBoxHeightOnDepth[d.depth - 1] - stackInfo.headerSize);

        var rect = enteringNodes.append('rect')
            .attr('class', 'box')
            .attr('x', -infoPackage.maxBoxWidth / 2)
            .attr('y', d => -1 * d.boxHeight() - stackInfo.headerSize)
            .attr('width', infoPackage.maxBoxWidth)
            .attr('height', d => d.boxHeight() + stackInfo.headerSize)
            .attr("fill", "red")
            .attr("rx", 5)
            .on('mouseout', () => nv.tooltip.cleanup());

        var clipPaths = this.svgDefs.selectAll('.stackClip').data(this.nodes);
        clipPaths
            .enter()
            .append("clipPath")
            .attr('class', 'stackClip')
            .attr('id', (d, i) => 'stack-clip-path-' + i)
            .append('rect')
            .attr('x', -infoPackage.maxBoxWidth / 2)
            .attr('width', infoPackage.maxBoxWidth - 5) // we substract little padding
            .attr('y', d => -1 * d.boxHeight() - stackInfo.headerSize)
            .attr('height', d => d.boxHeight() + stackInfo.headerSize);
                
        enteringNodes
            .append("text")
            .attr('text-anchor', 'middle')
            .attr('y', d => -1 * d.boxHeight())
            .text((d: stackInfo) => d.ThreadIds.length + " thread" + ((d.ThreadIds.length > 1) ? "s":''));

        enteringNodes
            .append("line")
            .attr('class', 'headerLine')
            .attr('x1', -1 * infoPackage.maxBoxWidth / 2)
            .attr('x2', infoPackage.maxBoxWidth / 2)
            .attr('y1', d => -1 * d.boxHeight() + 4)
            .attr('y2', d => -1 * d.boxHeight() + 4);
            
        enteringNodes.filter(d => d.depth > 0).each(function (d: stackInfo, index: number) {
            var g = this;
            var offsetTop = d.boxHeight() - stackInfo.boxPadding - stackInfo.lineHeight;
            var textGroup = d3.select(g)
                .append("g")
                .attr('class', 'traces')
                .style('clip-path', d => 'url(#stack-clip-path-' + index + ')');
            var stack = textGroup.selectAll('.trace').data(d.stackWithShortcuts().reverse());
            var reversedOriginalStack = d.StackTrace.reverse();
            stack
                .enter()
                .append('text')
                .attr('x', -140 + stackInfo.boxPadding)
                .attr('y', (d, i) => -offsetTop + stackInfo.lineHeight * i)
                .text(d => d.short)
                .classed('notUserCode', (s, i) => !stackInfo.isUserCode(reversedOriginalStack[i]))
                .on('mouseover', function(d) {
                    nv.tooltip.cleanup();
                    var offset = $(this).offset(); 
                    nv.tooltip.show([offset.left, offset.top], self.getTooltip(d), 'n', 25);
                });
        });

        var enteringLinks = (<any>this.link)
            .enter()
            .append("g");

        enteringLinks
            .append("path")
            .attr("class", "link")
            .attr("d", this.diagonal);
    }

    zoom() {
        this.svg.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
    }

    cleanup() {
        nv.tooltip.cleanup();
        $("#parallelStacks").empty();
        this.infoPackage(null);
        this.infoPackageFilename(null);
        this.fetchException(null);
        this.stacksJson(null);
    }

    createPackageWithStacks() {
        eventsCollector.default.reportEvent("info-package", "create", "with-stacks");
        this.createPackage(true);
    }

    createPackageWithoutStacks() {
        eventsCollector.default.reportEvent("info-package", "create", "without-stacks");
        this.createPackage(false);
    }

    createPackage(includeStacks: boolean) {
        this.showLoadingIndicator(true); 
        this.cleanup();
        var activeDb = this.adminView() ? appUrl.getSystemDatabase() : (this.activeDatabase() || appUrl.getSystemDatabase());
        new getInfoPackage(activeDb, includeStacks)
            .execute()
            .done((data, filename) => {
                this.infoPackage(data);
                this.infoPackageFilename(filename);
                var zip = new jszip(data); 
                var stacks = zip.file("stacktraces.txt");
                if (stacks) {
                    var stacksText = stacks.asText();
                    var stacksJson = JSON.parse(stacksText);

                    stacksJson.forEach(item => {
                        item.StackTrace = item.StackTrace.reverse();
                    });

                    this.packageCreated(stacksJson);
                }
            })
            .always(() => this.showLoadingIndicator(false)); 
    }

    packageCreated(stacksJson) {
        this.showMoreDetails(false);
        if ('Error' in stacksJson) {
            this.fetchException('Unable to fetch info package: ' + stacksJson.Error);
            this.fetchExceptionDetails(stacksJson.Details);
            return;
        } else {
            this.fetchException(null);
            this.fetchExceptionDetails(null);
        }
        this.stacksJson(stacksJson);
        var collatedStacks = this.splitAndCollateStacks(stacksJson);
        this.updateGraph(collatedStacks);
    }

    saveAsSvg() {
        eventsCollector.default.reportEvent("info-package", "export", "svg");
        svgDownloader.downloadSvg(d3.select('#parallelStacks').node(), 'stacks.svg', (svgClone) => {
            this.cleanupSvgCloneForSave(svgClone);
            return infoPackage.stacksCss;
        });
    }

    saveAsPng() {
        eventsCollector.default.reportEvent("info-package", "export", "png");
        svgDownloader.downloadPng(d3.select('#parallelStacks').node(), 'stacks.png', (svgClone) => {
            this.cleanupSvgCloneForSave(svgClone);
            return infoPackage.stacksCss;
        });
    }

    saveAsZip() { 
        eventsCollector.default.reportEvent("info-package", "export", "zip");
        fileDownloader.downloadAsZip(this.infoPackage(), this.infoPackageFilename());
    }

    saveAsJson() {
        eventsCollector.default.reportEvent("info-package", "export", "json");
        fileDownloader.downloadAsJson(this.stacksJson(), "stacks.json");
    }

    cleanupSvgCloneForSave(svgClone: Element) {
        d3.select(svgClone).select("g").select("g").attr('transform', null);
        var overlay = d3.select(svgClone).select(".overlay");
        
        d3.select(svgClone).attr('viewBox', '0 0 ' + (this.width + 50) + ' ' + (this.height + 20));
        d3.select(svgClone).select(".overlay").remove();
    }

    chooseImportFile() {
        eventsCollector.default.reportEvent("info-package", "import");
        var dialog = new infoPackageImport();
        dialog.task()
            .done((importedData: any) => {
                this.cleanup();
                if (importedData) {
                    this.packageCreated(importedData);
                } else {
                    messagePublisher.reportWarning("Stacktraces are not available in given file. Please create info package with stacktraces.");
                }
            })
            .fail(e => messagePublisher.reportError(e));
         
        app.showDialog(dialog);
    }

    moreDetails() {
        this.showMoreDetails(true);
    }
    

    static stacksCss = "svg { background-color: white; }\n" +
        " * { box-sizing: border-box; }\n" +
        " svg text { font-style: normal; font-variant: normal; font-weight: normal; font-size: 12px; line-height: normal; font-family: Arial; }\n" +
        " * { box-sizing: border - box; }\n" +
        " .link { fill: none; stroke: rgb(204, 204, 204); stroke-width: 1.5px; }\n" +
        " .overlay { fill: none; pointer-events: all; }\n" +
        " .box { fill: rgb(21, 140, 186); fill-opacity: 0.2; stroke: rgb(21, 140, 186); stroke-width: 1.5px; }\n" +
        " .notUserCode { fill-opacity: 0.4; }\n" +
        " .headerLine { stroke: rgb(21, 140, 186); stroke-width: 1.5px; }";

}

export = infoPackage;
