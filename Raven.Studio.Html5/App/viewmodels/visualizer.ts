import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");

import router = require("plugins/router");

import database = require("models/database");

import viewModelBase = require("viewmodels/viewModelBase");

import queryIndexDebugDocsCommand = require("commands/queryIndexDebugDocsCommand");
import queryIndexDebugMapCommand = require("commands/queryIndexDebugMapCommand");
import queryIndexDebugReduceCommand = require("commands/queryIndexDebugReduceCommand");
import queryIndexDebugAfterReduceCommand = require("commands/queryIndexDebugAfterReduceCommand");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");

import d3 = require('d3/d3');
import nv = require('nvd3');

class visualizer extends viewModelBase {

    indexes = ko.observableArray<{ name: string; hasReduce: boolean }>();
    indexName = ko.observable("Index Name");

    docKey = ko.observable("");
    docKeysSearchResults = ko.observableArray<string>(); 

    reduceKey = ko.observable("");
    reduceKeysSearchResults = ko.observableArray<string>(); 

    hasIndexSelected = ko.computed(() => {
        return this.indexName() !== "Index Name";
    });

    colors = d3.scale.category10();
    colorMap = {};

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

    node = null; // nodes selection
    link = null; // links selection

    activate(args) {
        super.activate(args);

        if (args && args.index) {
            this.indexName(args.index);
        }

        this.editIndexUrl = ko.computed(() => {
            return appUrl.forEditIndex(this.indexName(), this.activeDatabase());
        });

        this.runQueryUrl = ko.computed(() => {
            return appUrl.forQuery(this.activeDatabase(), this.indexName());
        });
        this.reduceKey.throttle(250).subscribe(search => this.fetchReduceKeySearchResults(search));
        this.docKey.throttle(250).subscribe(search => this.fetchDocKeySearchResults(search));

        return this.fetchAllIndexes();
    }

    attached() {
        this.resetChart();
        var svg = d3.select("#visualizer");
        this.diagonal = d3.svg.diagonal().projection(d => [d.y, d.x]);
        this.node = svg.selectAll(".node");
        this.link = svg.selectAll(".link");

        $("#visualizerContainer").resize().on('DynamicHeightSet', () => this.onWindowHeightChanged());
        this.width = $("#visualizerContainer").width();
        this.height = $("#visualizerContainer").height();
        this.updateScale();
        this.drawHeader();
    }

    resetChart() {
        this.tree = {
            level: 5,
            name: 'root',
            children: []
        }
        this.colorMap = {};
    }

    fetchReduceKeySearchResults(query: string) {
        if (query.length >= 2) {
            new queryIndexDebugMapCommand(this.indexName(), this.activeDatabase(), { startsWith: query }, 0, 10)
                .execute()
                .done((results: string[]) => {
                    if (this.reduceKey() === query) {
                        this.reduceKeysSearchResults(results.sort());
                    }
                });
        } else if (query.length == 0) {
            this.reduceKeysSearchResults.removeAll();
        }
    }

    fetchDocKeySearchResults(query: string) {
        if (query.length >= 2) {
            new queryIndexDebugDocsCommand(this.indexName(), this.activeDatabase(), query, 0, 10)
                .execute()
                .done((results: string[]) => {
                    if (this.docKey() === query) {
                        this.docKeysSearchResults(results.sort());
                    }
                });
        } else if (query.length == 0) {
            this.docKeysSearchResults.removeAll();
        }
    }

    updateScale() {
        this.xScale = d3.scale.linear().domain([0, 5]).range([this.margin.left, this.width - this.margin.left - this.margin.right]);
        this.boxWidth = this.width / 7;
    }

    drawHeader() {
        var header = d3.select("#visHeader");
        var headerData = ["Input", "Map", "Reduce 0", "Reduce 1", "Reduce 2"];
        var texts = header.selectAll("text").data(headerData);

        texts.transition().attr('x', (d, i) => this.xScale(i) + this.boxWidth / 2);

        texts.enter()
            .append("text")
            .attr('y', 20)
            .attr('x', (d, i) => this.xScale(i) + this.boxWidth / 2)
            .text(d => d).attr("text-anchor", "middle");

    }

    detached() {
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
        new queryIndexDebugMapCommand(this.indexName(), this.activeDatabase(), { sourceId: key }, 0, 1024)
            .execute()
            .then(results => {
                results.forEach(r => this.addReduceKey(r));
            });
    }

    addReduceKey(key:string) {
        var self = this;
        if (key && !(key in this.colorMap)) {

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
            });
        }
    }

    setSelectedIndex(indexName) {
        this.indexName(indexName);
        this.reduceKey("");
        this.docKey("");
        this.resetChart();
        this.updateGraph();
    }

    fetchAllIndexes(): JQueryPromise<any> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results: databaseStatisticsDto) => this.indexes(results.Indexes.map(i=> {
                return {
                    name: i.PublicName,
                    hasReduce: !!i.LastReducedTimestamp
                };
            }).filter(i => i.hasReduce)));
    }

    // replace characters with their char codes, but leave A-Za-z0-9 and - in place. 
    escape(input) {
        var output = "";
        for (var i = 0; i < input.length; i++) {
            var ch = input.charCodeAt(i);
            if (ch == 0x2F) {
                output += '-';
            } else if (ch >= 0x30 && ch <= 0x39 || ch >= 0x41 && ch <= 0x5A || ch >= 0x61 && ch <= 0x7A || ch == 0x2D) {
                output += input[i];
            } else {
                output += ch;
            }
        }
        return output;
    }

    makeNodeId(data: visualizerDataObjectNodeDto) {
        if (data.level == 4) {
            return this.escape("node-" + data.level + "-" + data.payload.Data["__reduce_key"]);
        } else if (data.payload) { 
            return this.escape("node-" + data.level + "-" + data.payload.ReduceKey + "-" + data.payload.Source + "-" + data.payload.Bucket);
        } else {
            return this.escape("node-" + data.level + "-" + data.name);
        }
    }

    setHighlightToParent(node, highlighted) {
        if (node.level == 0) {
            node.connections.forEach(c => this.setHighlightToParent(c, highlighted));
        } else {
            while (node) {
                d3.select("." + this.makeNodeId(node)).select('rect').classed('highlight', highlighted);
                node = node.parent;
            }
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

    getTooltip(data: visualizerDataObjectNodeDto) {
        var dataFormatted = JSON.stringify(data.payload.Data, undefined, 2);
        var content = '<button type="button" class="close" data-bind="click: tooltipClose" aria-hidden="true">׳</button>' +
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
                    x: l.source.x + self.margin.top
                },
                target: {
                    y: self.xScale(l.target.level) + self.boxWidth,
                    x: l.target.x + self.margin.top
                }
            }
        });
        
        this.node = this.node.data(this.nodes);
        this.link = this.link.data(this.links);

        var existingNodes = (<any>this.node)
            .transition()
            .attr("transform", (d) => "translate(" + self.xScale(d.level) + "," + (d.x + this.margin.top) + ")");

        existingNodes.select("rect")
            .attr('width', self.boxWidth);

        existingNodes.select('text')
            .attr('x', self.boxWidth / 2);

        (<any>this.link)
            .transition()
            .attr("d", this.diagonal);

        (<any>this.link)
            .enter()
            .append("path")
            .attr("class", "link")
            .attr("d", this.diagonal);

        var enteringNodes = (<any>this.node).enter().append("g").attr("class", d => "node " + self.makeNodeId(d) + " node-level-" + d.level)
            .attr("transform", (d) => "translate(" + self.xScale(d.level) + "," + (d.x + this.margin.top) + ")")
            .classed("hidden", d => d.level > 4);

        enteringNodes.append("rect").attr('class', 'nodeRect')
            .attr('x', 0)
            .attr('y', -10) 
            .attr("fill", d => d.level > 0 ? self.colorMap[d.name] : 'white')
            .attr('width', self.boxWidth)
            .attr('height', 20)
            .attr('rx', 5)
            .on("click", function (d) {
                nv.tooltip.cleanup();
                if (d.level === 0) {
                    router.navigate(appUrl.forEditDoc(d.name, null, null, self.activeDatabase()));
                }
                else {
                    d3.select(this).classed("hover", true);
                    var offset = $(this).offset();
                    nv.tooltip.show([offset.left + self.boxWidth / 2, offset.top], self.getTooltip(d), 'n', 25, null, "selectable-tooltip");
                    $(".nvtooltip").each((i, elem) => {
                        ko.applyBindings(self, elem);
                    });
                }
            })
            .on("mouseenter", function (d) {
                self.setHighlightToParent(d, true);
            })
            .on("mouseout", function (d) {
                if (d.level > 0) {
                    d3.select(this).classed("hover", false);
                }
                self.setHighlightToParent(d, false);
                
            });

        enteringNodes.append("text")
            .attr("x", self.boxWidth / 2)
            .attr("y", 4.5)
            .attr("pointer-events", "none")
            .attr("text-anchor", "middle")
            .text(d => d.name);

        this.node.exit().transition().attr('opacity', 0).remove();
        this.link.exit().transition().attr('opacity', 0).remove();
    }

    fetchDataFor(key: string) {
        var allDataFetched = $.Deferred <visualizerDataObjectNodeDto>();

        // TODO support for paging

        var mapTask = new queryIndexDebugMapCommand(this.indexName(), this.activeDatabase(), { key: key }, 0, 1024).execute();
        var reduce1Task = new queryIndexDebugReduceCommand(this.indexName(), this.activeDatabase(), 1, key, 0, 1024).execute();
        var reduce2Task = new queryIndexDebugReduceCommand(this.indexName(), this.activeDatabase(), 2, key, 0, 1024).execute();
        var indexEntryTask = new queryIndexDebugAfterReduceCommand(this.indexName(), this.activeDatabase(), [key]).execute();

        (<any>$.when(mapTask, reduce1Task, reduce2Task, indexEntryTask)).then((map: mappedResultInfo[], reduce1: mappedResultInfo[], reduce2: mappedResultInfo[], indexEntries: any[]) =>
        {

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
                    children:  subTree 
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

    selectReduceKey(value: string) {
        this.addReduceKey(value);
        this.reduceKey("");
    }

    selectDocKey(value: string) {
        this.addDocKey(value);
        this.docKey("");
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
            if (link.target.level == 0) {
                if (!("connections" in link.target)) {
                    link.target.connections = [];
                }
                var newTarget = seenNames[link.target.name];
                link.target = newTarget;
                link.target.connections.push(link.source);
                return link;
            } else {
                return link;
            }
        });
    }
}

export = visualizer;