import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");

import database = require("models/database");

import viewModelBase = require("viewmodels/viewModelBase");

import queryIndexDebugMapCommand = require("commands/queryIndexDebugMapCommand");
import queryIndexDebugReduceCommand = require("commands/queryIndexDebugReduceCommand");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");

import d3 = require('d3/d3');
import nv = require('nvd3');

class visualizer extends viewModelBase {

    indexes = ko.observableArray<{ name: string; hasReduce: boolean }>();
    indexName = ko.observable("Index Name");
    itemKey = ko.observable("");

    keys = ko.observableArray<string>();
    colors = d3.scale.category10();
    keysCounter = 0;

    tree: visualizerDataObjectNodeDto = null;
    xScale: D3.Scale.LinearScale;

    diagonal: any;
    graph: any;
    nodes: any[] = []; // nodes data
    links: any[] = []; // links data
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
        this.keys([]);
        this.keysCounter = 0;
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
    }

    onWindowHeightChanged() {
        this.width = $("#visualizerContainer").width();
        this.updateScale();
        this.drawHeader();
        this.updateGraph();
    }

    addItem() {
        var self = this;
        var key = this.itemKey();
        if (key && !this.keys.contains(key)) {
            this.keys.push(key);
            this.keysCounter++;
            this.itemKey("");

            this.fetchDataFor(key).then((subTree: visualizerDataObjectNodeDto[]) => {
                if (self.tree.children === undefined) {
                    self.tree.children = [];
                }
                self.tree.children.push({
                    level: 4,
                    name: key,
                    children: subTree
                });
                self.updateGraph();
            });
        }
    }

    setSelectedIndex(indexName) {
        this.indexName(indexName);
        this.itemKey("");
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
        if (data.payload) { 
            return this.escape("node-" + data.level + "-" + data.payload.ReduceKey + "-" + data.payload.Source + "-" + data.payload.Bucket);
        } else {
            return this.escape("node-" + data.level + "-" + data.name);
        }
    }

    setHighlightToParent(node, highlighted) {
        while (node) {
            d3.select("." + this.makeNodeId(node)).select('rect').classed('highlight', highlighted);
            node = node.parent;
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
        return "<table>" + 
            "<tr><td><strong>Reduce Key</strong></td><td>" + data.payload.ReduceKey + "</td></tr>" +
            "<tr><td><strong>Timestamp</strong></td><td>" + data.payload.Timestamp + "</td></tr>" +
            "<tr><td><strong>Etag</strong></td><td>" + data.payload.Etag + "</td></tr>" +
            "<tr><td><strong>Bucket</strong></td><td>" + data.payload.Bucket + "</td></tr>" +
            "<tr><td><strong>Source</strong></td><td>" + data.payload.Source + "</td></tr>" +
            "<tr><td><strong>Data</strong></td><td><pre>" + jsonUtil.syntaxHighlight(dataFormatted) + "</pre></td></tr>" +
        "</table>";
    }

    updateGraph() {
        this.height = this.estimateHeight();
        var self = this;

        d3.select("#visualizer")
            .attr("width", this.width)
            .attr("height", this.height)
            .attr("viewBox", "0 0 " + this.width + " " + this.height);

        this.graph = d3.layout.cluster()
            .size([this.height - this.margin.top - this.margin.bottom, this.width - this.margin.left - this.margin.right]);
        this.nodes = this.graph.nodes(this.tree);
        this.links = this.graph.links(this.nodes)
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

        var enteringNodes = (<any>this.node).enter().append("g").attr("class", d => "node " + self.makeNodeId(d))
            .attr("transform", (d) => "translate(" + self.xScale(d.level) + "," + (d.x + this.margin.top) + ")")
            .classed("hidden", d => d.level > 4);

        enteringNodes.append("rect").attr('class', 'nodeRect')
            .attr('x', 0)
            .attr('y', -10)
            .attr("fill",  d => d.level > 0 ?  this.colors(this.keysCounter) : 'white')
            .attr('width', self.boxWidth)
            .attr('height', 20)
            .attr('rx', 5)
            .on("mouseenter", function (d) {
                if (d.level > 0) {
                    d3.select(this).classed("hover", true);
                    var offset = $(this).offset();
                    nv.tooltip.show([offset.left + self.boxWidth / 2, offset.top], self.getTooltip(d), 'n', 25);
                }
                self.setHighlightToParent(d, true);
            })
            .on("mouseout", function (d) {
                if (d.level > 0) {
                    d3.select(this).classed("hover", false);
                }
                self.setHighlightToParent(d, false);
                nv.tooltip.cleanup();
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
        var allDataFetched = $.Deferred();

        // TODO support for paging

        var mapTask = new queryIndexDebugMapCommand(this.indexName(), this.activeDatabase(), key, 0, 1024).execute();
        var reduce1Task = new queryIndexDebugReduceCommand(this.indexName(), this.activeDatabase(), 1, key, 0, 1024).execute();
        var reduce2Task = new queryIndexDebugReduceCommand(this.indexName(), this.activeDatabase(), 2, key, 0, 1024).execute();

        (<any>$.when(mapTask, reduce1Task, reduce2Task)).then((map: mappedResultInfo[], reduce1: mappedResultInfo[], reduce2: mappedResultInfo[]) =>
        {
            var mapGroupedByBucket = d3
                .nest()
                .key(k => String(k.Bucket))
                .map(map, d3.map);

            var reduce1GropedByBucket = d3
                .nest()
                .key(d => String(d.Bucket))
                .map(reduce1, d3.map);

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
                allDataFetched.resolve(subTree);
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
                allDataFetched.resolve(subTree);
            }
        }, () => {
                allDataFetched.reject();
                });
        return allDataFetched;
    }
}

export = visualizer;