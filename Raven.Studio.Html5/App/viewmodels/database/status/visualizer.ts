/// <reference path="../../../../Scripts/typings/jquery.fullscreen/jquery.fullscreen.d.ts"/>

import app = require("durandal/app");

import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import generalUtils = require("common/generalUtils");
import svgDownloader = require("common/svgDownloader");
import fileDownloader = require("common/fileDownloader");
import messagePublisher = require("common/messagePublisher");
import chunkFetcher = require("common/chunkFetcher");

import router = require("plugins/router");
import visualizerKeys = require("viewmodels/database/status/visualizerKeys");
import visualizerImport = require("viewmodels/database/status/visualizerImport");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");

import queryIndexDebugDocsCommand = require("commands/database/debug/queryIndexDebugDocsCommand");
import queryIndexDebugMapCommand = require("commands/database/debug/queryIndexDebugMapCommand");
import queryIndexDebugReduceCommand = require("commands/database/debug/queryIndexDebugReduceCommand");
import queryIndexDebugAfterReduceCommand = require("commands/database/debug/queryIndexDebugAfterReduceCommand");
import getIndexNamesCommand = require("commands/database/index/getIndexNamesCommand");

import dynamicHeightBindingHandler = require("common/bindingHelpers/dynamicHeightBindingHandler");

import d3 = require('d3/d3');
import nv = require('nvd3');

class visualizer extends viewModelBase {

    static chooseIndexText = "Select an index";
    indexes = ko.observableArray<indexDataDto>();
    indexName = ko.observable(visualizer.chooseIndexText);

    onlineMode = ko.observable(true);
    showLoadingIndicator = ko.observable(false);

    docKey = ko.observable("");
    hasFocusDocKey = ko.observable<boolean>(false);
    loadingDocKeySearchResults = ko.observable<boolean>(false);
    docKeys = ko.observableArray<string>();
    docKeysSearchResults = ko.observableArray<string>();

    reduceKey = ko.observable("");
    hasFocusReduceKey = ko.observable<boolean>(false);
    loadingReduceKeySearchResults = ko.observable<boolean>(false);
    reduceKeys = ko.observableArray<string>();
    reduceKeysSearchResults = ko.observableArray<string>();

    hasIndexSelected = ko.computed(() => this.indexName() !== visualizer.chooseIndexText);
    docKeysOrReduceKeysSelected = ko.computed(() => this.docKeys().length > 0 || this.reduceKeys().length > 0);

    colors = d3.scale.category10();
    colorMap = {};

    selectedDocs = ko.observableArray<visualizerDataObjectNodeDto>();

    currentlySelectedNodes = d3.set([]);
    currentlySelectedLinks = d3.set([]);

    tree: visualizerDataObjectNodeDto = null;
    xScale: D3.Scale.LinearScale;

    editIndexUrl: KnockoutComputed<string>;
    runQueryUrl: KnockoutComputed<string>;

    diagonal: any;
    graph: any;
    nodes: visualizerDataObjectNodeDto[] = []; // nodes data
    links: graphLinkDto[] = []; // links data
    width: number;
    height: number;
    margin = {
        left: 10,
        right: 10,
        bottom: 10,
        top: 10
    }
    boxWidth: number;
    boxSpacing = 30;

    node: D3.Selection = null; // nodes selection
    link: D3.Selection = null; // links selection

    hasSaveAsPngSupport = ko.computed(() => {
        return !(navigator && navigator.msSaveBlob);
    });

    activate(args) {
        super.activate(args);

        if (args && args.index) {
            this.indexName(args.index);
        }

        this.updateHelpLink('RNXYCB');

        var db = this.activeDatabase();
        this.editIndexUrl = ko.computed(() => {
            return appUrl.forEditIndex(this.indexName(), db);
        });

        this.runQueryUrl = ko.computed(() => {
            return appUrl.forQuery(db, this.indexName());
        });

        this.hasFocusDocKey.subscribe(value => {
            if (value === false) {
                return;
            }
            this.fetchDocKeySearchResults("");
        });
        this.docKey.throttle(100).subscribe(search => this.fetchDocKeySearchResults(search));

        this.hasFocusReduceKey.subscribe(value => {
            if (value === false) {
                return;
            }
            this.fetchReduceKeySearchResults("");
        });
        this.reduceKey.throttle(100).subscribe(search => this.fetchReduceKeySearchResults(search));
        
        this.selectedDocs.subscribe(() => this.repaintSelectedNodes());

        $(document).bind("fullscreenchange", function () {
            if ($(document).fullScreen()) {
                $("#fullScreenButton i").removeClass("fa-expand").addClass("fa-compress");
            } else {
                $("#fullScreenButton i").removeClass("fa-compress").addClass("fa-expand");
            }
        });

        return this.fetchAllIndexes(db);
    }

    attached() {
        super.attached();
        var that = this;
        this.createKeyboardShortcut("esc", () => that.tooltipClose(), "body");

        this.resetChart();
        var svg = d3.select("#visualizer");
        this.diagonal = d3.svg.diagonal()
            .projection(d => [d.y, d.x]);
        this.node = svg.selectAll(".node");
        this.link = svg.selectAll(".link");

        $("#visualizerContainer").resize().on('DynamicHeightSet', () => this.onWindowHeightChanged());
        this.width = $("#visualizerContainer").width();
        this.height = $("#visualizerContainer").height();
        this.updateScale();
        this.drawHeader();
    }

    compositionComplete() {
        super.compositionComplete();
        this.hasIndexSelected.subscribe(() => {
            dynamicHeightBindingHandler.forceUpdate();
        });
    }

    resetChart() {
        this.tooltipClose();
        this.reduceKey("");
        this.reduceKeys([]);
        this.docKey("");
        this.docKeys([]);
        this.tree = {
            level: 5,
            name: 'root',
            children: []
        }
        this.colorMap = {};

        this.currentlySelectedNodes = d3.set([]);
        this.currentlySelectedLinks = d3.set([]);
    }

    fetchDocKeySearchResults(query: string) {
        this.loadingDocKeySearchResults(true);

        new queryIndexDebugDocsCommand(this.indexName(), this.activeDatabase(), query, 0, 10)
            .execute()
            .done((results: string[]) => {
                if (this.docKey() === query) {
                    this.docKeysSearchResults(results.sort());
                }
            })
            .always(() => this.loadingDocKeySearchResults(false));
    }

    fetchReduceKeySearchResults(query: string) {
        this.loadingReduceKeySearchResults(true);

        new queryIndexDebugMapCommand(this.indexName(), this.activeDatabase(), { startsWith: query }, 0, 10)
            .execute()
            .done((results: string[]) => {
                if (this.reduceKey() === query) {
                    this.reduceKeysSearchResults(results.sort());
                }
            })
            .always(() => this.loadingReduceKeySearchResults(false));
    }

    updateScale() {
        this.xScale = d3.scale.linear().domain([0, 5]).range([this.margin.left, this.width - this.margin.left - this.margin.right]);
        this.boxWidth = this.width / 7;
    }

    drawHeader() {
        var header = d3.select("#visHeader");
        var headerData = ["Input", "Map", "Reduce 0", "Reduce 1", "Reduce 2"];
        var texts = header.selectAll("text").data(headerData);

        texts.attr('x', (d, i) => this.xScale(i) + this.boxWidth / 2);

        texts
            .enter()
            .append("text")
            .attr('y', 20)
            .attr('x', (d, i) => this.xScale(i) + this.boxWidth / 2)
            .text(d => d)
            .attr("text-anchor", "middle");
    }

    detached() {
        super.detached();

        $("#visualizerContainer").off('DynamicHeightSet');
        nv.tooltip.cleanup();
    }

    onWindowHeightChanged() {
        this.width = $("#visualizerContainer").width();
        this.updateScale();
        this.drawHeader();
        this.updateGraph();
    }

    addDocKey(key: string) {
        if (!key || this.docKeys.contains(key))
            return;

        this.showLoadingIndicator(true);
        new queryIndexDebugDocsCommand(this.indexName(), this.activeDatabase(), key, 0, 1)
            .execute()
            .done(results => {
                if (results && results.length === 1 && results[0] == key) {
                    if (key && !this.docKeys.contains(key)) {
                        this.docKeys.push(key);
                        new queryIndexDebugMapCommand(this.indexName(), this.activeDatabase(), { sourceId: key }, 0, 1024)
                            .execute()
                            .then(results => {
                                var addReduceKeysTasks: Array<JQueryPromise<any[]>> = [];
                                results.forEach(r => addReduceKeysTasks.push(this.addReduceKey(r, false)));

                                $.when.apply($, addReduceKeysTasks).done(() => {
                                    this.showLoadingIndicator(false);
                                });
                            });
                    }
                } else {
                    messagePublisher.reportWarning("The document, " + key + " , is not contained in the documents for this index.");
                    this.showLoadingIndicator(false);
                }
            });        
    }

    addReduceKey(key: string, needToChangeLoadingIndicator = true): JQueryPromise<any[]> {
        var deferred = $.Deferred();
        if (!key || this.reduceKeys().contains(key))
            return deferred.resolve();

        if (needToChangeLoadingIndicator)
            this.showLoadingIndicator(true);
        var self = this;

        var getTask = new queryIndexDebugMapCommand(this.indexName(), this.activeDatabase(), { startsWith: key }, 0, 1).execute();
        getTask.done((x) => {
                if (x) {
                    if (key && !this.reduceKeys().contains(key)) {
                        this.reduceKeys.push(key);

                        this.colorMap[key] = this.colors(Object.keys(this.colorMap).length);
                        this.reduceKey("");

                        this.fetchDataFor(key).then((subTree: visualizerDataObjectNodeDto) => {
                            if (self.tree.children === undefined) {
                                self.tree.children = [];
                            }
                            if (subTree.children.length > 0) {
                                self.tree.children.push(subTree);
                                self.updateGraph();
                            }
                        }).always(() => {
                            if (needToChangeLoadingIndicator)
                                this.showLoadingIndicator(false);
                            deferred.resolve();
                        });
                    } else {
                        if (needToChangeLoadingIndicator)
                            this.showLoadingIndicator(false);
                        deferred.resolve();
                    }
                } else {
                    messagePublisher.reportWarning("The key, " + key + " , is not contained in the reduce keys for this index.");
                    deferred.resolve();
                }
            }).always(() => {
                if (needToChangeLoadingIndicator)
                    this.showLoadingIndicator(false);
                deferred.resolve();
            });

        return deferred;
    }

    setSelectedIndex(indexName) {
        this.indexName(indexName);
        this.resetChart();
        this.updateGraph();
    }

    clearChart() {
        this.indexName(visualizer.chooseIndexText);
        this.onlineMode(true);
        this.resetChart();
        this.updateGraph();
    }

    fetchAllIndexes(db: database): JQueryPromise<any> {
        return new getIndexNamesCommand(this.activeDatabase(), true)
            .execute()
            .done((results: indexDataDto[]) => this.indexes(results.filter(i => i.IsMapReduce)));
    }

    static makeLinkId(link: graphLinkDto) {
        if ("cachedId" in link) {
            return link.cachedId;
        } else {
            var result = "link-" + visualizer.makeNodeId(link.source.origin) + "-" + visualizer.makeNodeId(link.target.origin);
            link.cachedId = result;
            return result;
        }
    }

    static makeNodeId(data: visualizerDataObjectNodeDto) {
        if ("cachedId" in data) {
            return data.cachedId;
        } else {
            var nodeId = null;
            if (data.level == 4) {
                nodeId = "node-" + data.level + "-" + data.payload.Data["__reduce_key"];
            } else if (data.payload) {
                nodeId = "node-" + data.level + "-" + data.payload.ReduceKey + "-" + data.payload.Source + "-" + data.payload.Bucket;
            } else {
                nodeId = "node-" + data.level + "-" + data.name;
            }
            nodeId = generalUtils.escape(nodeId);
            data.cachedId = nodeId;
            return nodeId;
        }
    }

    estimateHeight() {
        var level1Nodes = 0;
        var nodes = [this.tree];
        var node = null;
        while ((node = nodes.pop()) != null) {
            if (node.level == 1) level1Nodes++;
            if ((children = node.children) && (n = children.length)) {
                var n, children;
                while (--n >= 0) nodes.push(children[n]);
            }
        }
        return this.boxSpacing * level1Nodes + this.margin.top + this.margin.bottom;
    }

    goToDoc(docName) {
        router.navigate(appUrl.forEditDoc(docName, null, null, this.activeDatabase()));
    }

    getTooltip(data: visualizerDataObjectNodeDto) {
        var content = "";
        if (data.level == 0) {
            content += '<button data-bind="click: goToDoc.bind($root, \'' + data.name + '\')" class="btn" type="button">Go to document</button>';
        } else {

            var dataFormatted = JSON.stringify(data.payload.Data, undefined, 2);
            content += '<button type="button" class="close" data-bind="click: tooltipClose" aria-hidden="true"><i class="fa fa-times"></i></button>' +
            "<table> ";

            if (data.level < 4) {
                content += "<tr><td><strong>Reduce Key</strong></td><td>" + data.payload.ReduceKey + "</td></tr>" +
                "<tr><td><strong>Timestamp</strong></td><td>" + data.payload.Timestamp + "</td></tr>" +
                "<tr><td><strong>Etag</strong></td><td>" + data.payload.Etag + "</td></tr>" +
                "<tr><td><strong>Bucket</strong></td><td>" + data.payload.Bucket + "</td></tr>" +
                "<tr><td><strong>Source</strong></td><td>" + data.payload.Source + "</td></tr>";
            }

            content += "<tr><td><strong>Data</strong></td><td><pre>" + jsonUtil.syntaxHighlight(dataFormatted) + "</pre></td></tr>" +
            "</table>";
        }
        return content;
    }

    updateGraph() {
        this.height = this.estimateHeight();
        var self = this;

        d3.select("#visualizer")
            .style({ height: self.height + 'px' })
            .attr("viewBox", "0 0 " + this.width + " " + this.height);

        this.graph = d3.layout.cluster()
            .size([this.height - this.margin.top - this.margin.bottom, this.width - this.margin.left - this.margin.right]);
        this.nodes = this.graph.nodes(this.tree);
        this.links = this.graph.links(this.nodes);
        this.remapNodesAndLinks();
        this.links = this.links
            .filter(l => l.target.level < 4)
            .map(l => {
            return {
                    source: {
                        y: self.xScale(l.source.level),
                        x: l.source.x + self.margin.top,
                        origin: l.source
                    },
                    target: {
                        y: self.xScale(l.target.level) + self.boxWidth,
                        x: l.target.x + self.margin.top,
                        origin: l.target
                    }
                }
        });

        this.node = this.node.data(this.nodes);
        this.link = this.link.data(this.links);

        var existingNodes = (<any>this.node)
            .attr("transform", (d) => "translate(" + self.xScale(d.level) + "," + (d.x + this.margin.top) + ")");

        existingNodes.select("rect")
            .attr('width', self.boxWidth);

        existingNodes.select('text')
            .attr('x', self.boxWidth / 2);

        (<any>this.link)
            .attr("d", this.diagonal);

        (<any>this.link)
            .enter()
            .append("path")
            .attr("class", "link")
            .attr("id", visualizer.makeLinkId)
            .attr("d", this.diagonal);

        var enteringNodes = (<D3.UpdateSelection>this.node)
            .enter()
            .append("g")
            .attr("id", visualizer.makeNodeId)
            .attr("transform", (d) => "translate(" + self.xScale(d.level) + "," + (d.x + this.margin.top) + ")");

        enteringNodes
            .filter(d => d.level > 4)
            .classed("hidden", true);

        enteringNodes
            .append("rect")
            .attr('class', 'nodeRect')
            .attr('x', 0)
            .attr('y', -10)
            .attr("fill", d => d.level > 0 ? self.colorMap[d.name] : 'white')
            .attr('width', self.boxWidth)
            .attr('height', 20)
            .attr('rx', 5)
            .on("click", function (d) {
                nv.tooltip.cleanup();
                var offset = $(this).offset();
                var containerOffset = $("#visualizerSection").offset();
                nv.tooltip.show([offset.left - containerOffset.left + self.boxWidth / 2, offset.top - containerOffset.top], self.getTooltip(d), 'n', 25, document.getElementById("visualizerSection"), "selectable-tooltip");
                $(".nvtooltip").each((i, elem) => {
                    ko.applyBindings(self, elem);
                });
            });

        enteringNodes
            .filter(d => d.level <= 3)
            .append("rect")
            .attr('class', 'nodeCheck')
            .attr('x', 3)
            .attr('y', -7)
            .attr('width', 14)
            .attr('height', 14)
            .attr('rx', 2)
            .on("click", function (d) {
                nv.tooltip.cleanup();
                var selected = this.classList.contains("selected");
                d3.select(this).classed("selected", !selected);

                if (selected) {
                    self.selectedDocs.remove(d);
                } else {
                    if (!self.selectedDocs.contains(d)) {
                        self.selectedDocs.push(d);
                    }
                }
            });

        enteringNodes.append("text")
            .attr("x", self.boxWidth / 2)
            .attr("y", 4.5)
            .text(d => self.trimTextIfNeeded(d.name));

        (<D3.UpdateSelection>this.node).exit().remove();
        (<D3.UpdateSelection>this.link).exit().remove();
    }

    trimTextIfNeeded(input: string) {
        return input.length > 25 ? input.substring(0, 25) + "..." : input;
    }

    fetchDataFor(key: string) {
        var allDataFetched = $.Deferred<visualizerDataObjectNodeDto>();

        var mapFetcherTask = new chunkFetcher<mappedResultInfo>((skip, take) => new queryIndexDebugMapCommand(this.indexName(), this.activeDatabase(), { key: key }, skip, take).execute()).execute();
        var reduce1FetcherTask = new chunkFetcher<mappedResultInfo>((skip, take) => new queryIndexDebugReduceCommand(this.indexName(), this.activeDatabase(), 1, key, skip, take).execute()).execute();
        var reduce2FetcherTask = new chunkFetcher<mappedResultInfo>((skip, take) => new queryIndexDebugReduceCommand(this.indexName(), this.activeDatabase(), 2, key, skip, take).execute()).execute();
        var indexEntryTask = new queryIndexDebugAfterReduceCommand(this.indexName(), this.activeDatabase(), [key]).execute();

        (<any>$.when(mapFetcherTask, reduce1FetcherTask, reduce2FetcherTask, indexEntryTask))
            .then((map: mappedResultInfo[], reduce1: mappedResultInfo[], reduce2: mappedResultInfo[], indexEntries: any[]) => {

                if (map.length == 0 && reduce1.length == 0 && reduce2.length == 0) {
                    allDataFetched.resolve({
                        level: 4,
                        name: key,
                        children: []
                    });
                    return;
                }

                var mapGroupedByBucket = d3
                    .nest()
                    .key(k => String(k.Bucket))
                    .map(map, d3.map);

                var reduce1GropedByBucket = d3
                    .nest()
                    .key(d => String(d.Bucket))
                    .map(reduce1, d3.map);

                var indexEntry = indexEntries[0];

                if (reduce2.length == 0 && reduce1.length == 0) {
                    var subTree: visualizerDataObjectNodeDto[] = map.map((m: mappedResultInfo) => {
                        return {
                            name: m.ReduceKey,
                            payload: m,
                            level: 1,
                            children: [
                                {
                                    name: m.Source,
                                    level: 0,
                                    children: []
                                }
                            ]
                        }
                    });

                    allDataFetched.resolve({
                        level: 4,
                        name: key,
                        payload: { Data: indexEntry },
                        children: subTree
                    });
                }

                if (reduce2.length > 0 && reduce1.length > 0) {
                    var subTree: visualizerDataObjectNodeDto[] = reduce2.map(r2 => {
                        return {
                            name: r2.ReduceKey,
                            payload: r2,
                            level: 3,
                            children: reduce1GropedByBucket.get(r2.Source).map((r1: mappedResultInfo) => {
                                return {
                                    name: r1.ReduceKey,
                                    payload: r1,
                                    level: 2,
                                    children: mapGroupedByBucket.get(r1.Source).map((m: mappedResultInfo) => {
                                        return {
                                            name: m.ReduceKey,
                                            payload: m,
                                            level: 1,
                                            children: [
                                                {
                                                    name: m.Source,
                                                    level: 0,
                                                    children: []
                                                }
                                            ]
                                        }
                                    })
                                }
                            })
                        }
                    });
                    allDataFetched.resolve({
                        level: 4,
                        name: key,
                        payload: { Data: indexEntry },
                        children: subTree
                    });
                }
            }, () => {
                allDataFetched.reject();
            });
        return allDataFetched;
    }

    tooltipClose() {
        nv.tooltip.cleanup();
    }

    selectDocKey(value: string) {
        this.addDocKey(value);
        this.docKey("");
        this.docKeysSearchResults.removeAll();
    }

    selectReduceKey(value: string) {
        this.addReduceKey(value);
        this.reduceKey("");
        this.reduceKeysSearchResults.removeAll();
    }

    transiviteClosure() {
        var result = d3.set([]);
        var nodes = this.selectedDocs();
        var queue: visualizerDataObjectNodeDto[] = [];
        queue.pushAll(nodes);

        this.treeTranverse(queue, (node) => result.add(visualizer.makeNodeId(node)));

        return result;
    }

    treeTranverse(queue: visualizerDataObjectNodeDto[], callback: (node: visualizerDataObjectNodeDto) => void, refPropName: string = "connections") {
        var everQueued = d3.set([]);

        while (queue.length > 0) {
            var node = queue.shift();
            callback(node);

            if (node[refPropName]) {
                node[refPropName].forEach(c => {
                    var nodeId = visualizer.makeNodeId(c);
                    if (!everQueued.has(nodeId)) {
                        queue.push(c);
                        everQueued.add(nodeId);
                    }
                });
            }
        }
    }

    exportTree(root: visualizerDataObjectNodeDto): visualizerDataObjectNodeDto {
        return {
            level: root.level,
            name: root.name,
            payload: root.payload,
            children: root.children ? $.map(root.children, (v, i) => this.exportTree(v)) : undefined
        }
    }

    computeLinks(nodes: D3.Set) {
        var output = d3.set([]);

        this.links.forEach(link => {
            var node1 = visualizer.makeNodeId(link.source.origin);
            var node2 = visualizer.makeNodeId(link.target.origin);

            if (nodes.has(node1) && nodes.has(node2)) {
                output.add(visualizer.makeLinkId(link));
            }
        });
        return output;
    }

    repaintSelectedNodes() {

        var newClosure = this.transiviteClosure();
        var currentClosure = this.currentlySelectedNodes;
        var currentLinks = this.currentlySelectedLinks;
        var newLinks = this.computeLinks(newClosure);

        // remove all works in situ
        var incoming = newClosure.values();
        incoming.removeAll(currentClosure.values());
        var outgoing = currentClosure.values();
        outgoing.removeAll(newClosure.values());
        var incomingLinks = newLinks.values();
        incomingLinks.removeAll(currentLinks.values());
        var outgoingLinks = currentLinks.values();
        outgoingLinks.removeAll(newLinks.values());

        incoming.forEach(name => d3.select("#" + name).select('rect').classed("highlight", true));
        outgoing.forEach(name => d3.select("#" + name).select('rect').classed("highlight", false));
        incomingLinks.forEach(name => d3.select("#" + name).classed("selected", true));
        outgoingLinks.forEach(name => d3.select("#" + name).classed("selected", false));

        this.currentlySelectedNodes = newClosure;
        this.currentlySelectedLinks = newLinks;
    }

    remapNodesAndLinks() {
        var seenNames = {};
        var nodesToDelete = [];

        // process nodes
        this.nodes.forEach(node => {
            if (node.level == 0) {
                if (node.name in seenNames) {
                    nodesToDelete.push(node);
                } else {
                    seenNames[node.name] = node;
                }
            }
        });
        this.nodes.removeAll(nodesToDelete);

        // process links
        this.links = this.links.map(link => {
            if (!("connections" in link.target)) {
                link.target.connections = [];
            }
            if (link.target.level == 0) {
                var newTarget = seenNames[link.target.name];
                link.target = newTarget;
                if (!link.target.connections.contains(link.source)) {
                    link.target.connections.push(link.source);
                }
                return link;
            } else {
                if (!link.target.connections.contains(link.source)) {
                    link.target.connections.push(link.source);
                }
                return link;
            }
        });
    }

    toggleFullscreen(e) {
        if ($(document).fullScreen()) {
            $("#visualizerSection").width('').height('');
            $("#keysDialogBtn").removeAttr('disabled');
        } else {
            $("#visualizerSection").width("100%").height("100%");
            $("#keysDialogBtn").attr('disabled', 'disabled');
        }

        $("#visualizerSection").toggleFullScreen();
    }

    displayKeyInfo() {
        app.showDialog(new visualizerKeys(this));
    }

    saveAsSvg() {
        svgDownloader.downloadSvg(d3.select('#visualizer').node(), 'visualization.svg', (e) => visualizer.visualizationCss);
    }

    saveAsPng() {
        svgDownloader.downloadPng(d3.select('#visualizer').node(), 'visualization.png', (e) => visualizer.visualizationCss);
    }

    saveAsJson() {
        var model: visualizerExportDto = {
            indexName: this.indexName(),
            docKeys: this.docKeys(),
            reduceKeys: this.reduceKeys(),
            tree: this.exportTree(this.tree)
        };

        fileDownloader.downloadAsJson(model, "visualizer.json");
    }

    chooseImportFile() {
        var dialog = new visualizerImport();
        dialog.task().
            done((importedData: visualizerExportDto) => {
                this.clearChart();
                this.onlineMode(false);
                this.docKeys(importedData.docKeys);
                this.reduceKeys(importedData.reduceKeys);

                importedData.reduceKeys.forEach(reduceKey => {
                    this.colorMap[reduceKey] = this.colors(Object.keys(this.colorMap).length);
                });

                this.tree = importedData.tree;
                this.indexName(importedData.indexName);
                this.updateGraph();
            });

        app.showDialog(dialog);
    }


    static visualizationCss = 'svg { background-color: white; }\n' +
        '* { box-sizing: border-box; }\n' +
    '.hidden { display: none !important; visibility: hidden !important; }\n' +
    'svg text { font-style: normal; font-variant: normal; font-weight: normal; font-size: 12px; line-height: normal; font-family: Arial; }\n' +
    '.nodeRect { stroke: rgb(119, 119, 119); stroke-width: 1.5px; fill-opacity: 0.4 !important; }\n' +
    '.nodeCheck { stroke-width: 2px; stroke: rgb(0, 0, 0); fill: rgb(255, 255, 255); }\n' +
    '.hidden { display: none; }\n' +
    'g { font-style: normal; font-variant: normal; font-weight: normal; font-size: 10px; line-height: normal; font-family: sans - serif; cursor: pointer; }\n' +
    '.link { fill: none; stroke: rgb(204, 204, 204); stroke-width: 1.5px; }\n' +
    'text { pointer-events: none; text-anchor: middle; }\n' +
    '.link.selected { fill: none; stroke: black; stroke-width: 2.5px; } \n';

}

export = visualizer;
