/// <reference path="../../../Scripts/typings/d3/dagre.d.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import svgDownloader = require("common/svgDownloader");
import fileDownloader = require("common/fileDownloader");
import getGlobalReplicationTopology = require("commands/resources/getGlobalReplicationTopology");
import getDocumentsLeftToReplicate = require("commands/database/replication/getDocumentsLeftToReplicate");
import d3 = require('d3/d3');
import dagre = require('dagre');
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import shell = require("viewmodels/shell");
import database = require("models/resources/database");

class topology extends viewModelBase {

    static inlineCss = " svg { background-color: white; }" +
                        " path.link { fill: none; stroke: #38b44a; stroke-width: 5px; cursor: default; } " +
                        " path.link.error {  stroke: #df382c; } " +
" svg:not(.active):not(.ctrl) path.link { cursor: pointer; } " +
" path.link.hidden {  stroke-width: 0; } " +
" rect.node {  stroke-width: 1.5px;  fill: rgba(243, 101, 35, 0.15); stroke: #d74c0c;  } " + 
" text { font: 12px sans-serif;  pointer-events: none;  } " +
" text.id { text-anchor: middle;  font-weight: bold;  }";

    topology = ko.observable<globalTopologyDto>(null);
    topologyFiltered = ko.observable<globalTopologyDto>(null);
    currentLink = ko.observable<any>(null); 
    searchText = ko.observable<string>();

    documentToReplicateText = ko.observable<string>(null);
    isLoadingDocumentToReplicateCount = ko.observable<boolean>(false);
    localDatabaseIds: KnockoutComputed<string[]>;
    canCalculateDocumentsToReplicateCount: KnockoutComputed<boolean>;
    canExportDocumentsToReplicateCount: KnockoutComputed<boolean>;

    settingsAccess = new settingsAccessAuthorizer();

    fetchDb = ko.observable<boolean>(true);
    fetchFs = ko.observable<boolean>(true);
    fetchCs = ko.observable<boolean>(shell.has40Features());

    showLoadingIndicator = ko.observable(false); 

    dagreGraphSize: [number, number];

    showCsOption = shell.has40Features;

    width: number;
    height: number;
    svg: D3.Selection;
    zoom: D3.Behavior.Zoom;
    colors = d3.scale.category10();
    line = d3.svg.line().x(d => d.x).y(d => d.y);

    constructor() {
        super();

        this.searchText.throttle(250).subscribe(value => this.filter(value));

        this.localDatabaseIds = ko.computed(() => {
            var topology = this.topology();
            if (!topology) {
                return [];
            }

            return topology.Databases.LocalDatabaseIds;
        });

        this.canCalculateDocumentsToReplicateCount = ko.computed(() => {
            var currentLink = this.currentLink();
            if (!currentLink) {
                return false;
            }

            if (currentLink.SourceToDestinationState === "Offline") {
                return false;
            }

            var topology = this.topology();
            if (!topology) {
                return false;
            }

            var destinations = this.getAllReachableDestinationsFrom(currentLink.Source, topology.Databases.Connections);

            return destinations.contains(currentLink.Destination);
        });

        this.canExportDocumentsToReplicateCount = ko.computed(() => {
            var currentLink = this.currentLink();
            if (!currentLink) {
                return false;
            }

            if (currentLink.SourceToDestinationState === "Offline") {
                return false;
            }

            var topology = this.topology();
            if (!topology) {
                return false;
            }

            var localServer = topology
                .Databases
                .Connections
                .first((x: replicationTopologyConnectionDto) => {
                    var serverId = this.getServerId(x.SendServerId, x.StoredServerId);
                    var foundServerId = this.localDatabaseIds().first(id => id === serverId);
                    if (!foundServerId) {
                        return false;
                    }

                    return x.Source === currentLink.Source;
                });

            return !!localServer;
        });
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("ES8PCB");
    }

    attached() {
        super.attached();
        d3.select(window).on("resize", this.resize.bind(this));
    }

    compositionComplete() {
        this.resize();
    }

    getAllReachableDestinationsFrom(sourceServerUrl: string, connections: replicationTopologyConnectionDto[]): string[] {
        var result: string[] = [];

        connections.forEach(connection => {
            if (sourceServerUrl === connection.Source) {
                result.push(connection.Destination);

                var updatedConncetions = connections.filter(x => x.Destination !== sourceServerUrl);

                var reachables = this.getAllReachableDestinationsFrom(connection.Destination, updatedConncetions);
                result.pushAll(reachables);
            }
        });

        return result;
    }

    getDocumentsToReplicateCount() {
        var currentLink = this.currentLink();
        if (currentLink == null) {
            return;
        }
        
        if (currentLink.SourceToDestinationState === "Offline") {
            return;
        }

        var destinationSplitted = currentLink.Destination.split("/databases/");
        var databaseName = destinationSplitted.last();
        var destinationUrl = destinationSplitted.first() + "/";
        var sourceSplitted = currentLink.Source.split("/databases/");
        var sourceUrl = sourceSplitted.first() + "/";
        var sourceDatabaseName = sourceSplitted.last();
        var sourceId = this.getServerId(currentLink.SendServerId, currentLink.StoredServerId);

        this.isLoadingDocumentToReplicateCount(true);
        var getDocsToReplicateCount =
            new getDocumentsLeftToReplicate(sourceUrl, destinationUrl, databaseName,
                    sourceId, new database(sourceDatabaseName))
                .execute();

        getDocsToReplicateCount
            .done((documentCount: documentCountDto) => {
                var message = "";
                var isApproximate = documentCount.Type === "Approximate";
                if (isApproximate) {
                    message += "Approximately ";
                }

                message += documentCount.Count.toLocaleString();

                if (isApproximate) {
                    message += ">=";
                }

                if (documentCount.IsEtl) {
                    message += " (ETL)";
                }

                this.documentToReplicateText(message);
            })
            .fail(() => this.documentToReplicateText("Couldn't calculate document count!"))
            .always(() => this.isLoadingDocumentToReplicateCount(false));
    }

    getServerId(sendServerId: string, storedServerId: string) {
        var serverId = sendServerId;
        if (serverId === "00000000-0000-0000-0000-000000000000") {
            serverId = storedServerId;
        }

        return serverId;
    }

    export() {
        var confirmation = this.confirmationMessage("Export", "Are you sure that you want to export documents to replicate ids?");
        confirmation.done(() => {
            var url = "/admin/replication/export-docs-left-to-replicate";
            var currentLink = this.currentLink();
            if (currentLink == null) {
                return;
            }

            if (currentLink.SourceToDestinationState === "Offline") {
                return;
            }

            this.isLoadingDocumentToReplicateCount(true);
            var destinationSplitted = currentLink.Destination.split("/databases/");
            var databaseName = destinationSplitted.last();
            var destinationUrl = destinationSplitted.first() + "/";
            var sourceSplitted = currentLink.Source.split("/databases/");
            var sourceUrl = sourceSplitted.first() + "/";
            var sourceDatabaseName = sourceSplitted.last();

            var requestData = {
                SourceUrl: sourceUrl,
                DestinationUrl: destinationUrl,
                DatabaseName: databaseName,
                SourceId: this.getServerId(currentLink.SendServerId, currentLink.StoredServerId)
            };

            var db = new database(sourceDatabaseName);
            this.downloader.downloadByPost(db, url, requestData, this.isLoadingDocumentToReplicateCount);
        });
    }

    hasSaveAsPngSupport = ko.computed(() => {
        return !(navigator && navigator.msSaveBlob);
    });

    resize() {
        this.width = $("#replicationTopologySection").width() * 0.66;
    }

    createReplicationTopology() {
        var self = this;

        var $replicationTopologSection = $("#replicationTopologySection");
        var $replicationTopologySvg = $("svg#replicationTopology");

        this.height = $replicationTopologSection.outerHeight() -
            ($replicationTopologySvg.offset().top - $replicationTopologSection.offset().top) -
            20;

        this.zoom = d3.behavior.zoom()
            .scaleExtent([0.2, 5])
            .on("zoom", () => {
                this.svg.select('.graphZoom').attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
            });

        this.svg = d3.select("#replicationTopology")
            .style({ height: self.height + 'px' })
            .style({ width: self.width + 'px' })
            .attr("viewBox", "0 0 " + self.width + " " + self.height)
            .call(this.zoom);

        this.syncGraph();

    }

    private syncGraph() {
        var topologyGraph = new dagre.graphlib.Graph();
        topologyGraph.setGraph({});
        topologyGraph.setDefaultEdgeLabel(() => "");

        this.fillNodes(topologyGraph);

        dagre.layout(topologyGraph);
        this.renderTopology(topologyGraph);
    }

    private fillNodes(topologyGraph) {
        if (this.topologyFiltered().Databases) {
            this.topologyFiltered().Databases.SkippedResources.forEach(s => {
                topologyGraph.setNode(s, {
                    id: s,
                    rType: "db",
                    url: s,
                    name: ' (skipped)',
                    width: 220,
                    height: 40
                });
            });

            this.topologyFiltered().Databases.Servers.forEach(s => {
                topologyGraph.setNode(s, {
                    id: s,
                    rType: "db",
                    url: s.split("/databases/")[0],
                    name: s.split("/databases/")[1],
                    width: 220,
                    height: 40
                });
            });

            this.topologyFiltered().Databases.Connections.forEach(c => {
                c.UiType = "db";
                topologyGraph.setEdge(c.Source, c.Destination, c);
            });
        }

        if (this.topologyFiltered().FileSystems) {
            this.topologyFiltered().FileSystems.SkippedResources.forEach(s => {
                topologyGraph.setNode(s, {
                    id: s,
                    rType: "fs",
                    url: s,
                    name: " (skipped)",
                    width: 220,
                    height: 40
                });
            });

            this.topologyFiltered().FileSystems.Servers.forEach(s => {
                topologyGraph.setNode(s, {
                    id: s,
                    rType: "fs",
                    url: s.split("/fs/")[0],
                    name: s.split("/fs/")[1],
                    width: 220,
                    height: 40
                });
            });

            this.topologyFiltered().FileSystems.Connections.forEach(c => {
                c.UiType = "fs";
                topologyGraph.setEdge(c.Source, c.Destination, c);
            });
        }

        if (this.topologyFiltered().Counters) {
            this.topologyFiltered().Counters.SkippedResources.forEach(s => {
                topologyGraph.setNode(s, {
                    id: s,
                    rType: "cs",
                    url: s,
                    name: " (skipped)",
                    width: 220,
                    height: 40
                });
            });

            this.topologyFiltered().Counters.Servers.forEach(s => {
                topologyGraph.setNode(s, {
                    id: s,
                    rType: "cs",
                    url: s.split("/cs/")[0],
                    name: s.split("/cs/")[1],
                    width: 220,
                    height: 40
                });
            });

            this.topologyFiltered().Counters.Connections.forEach(c => {
                c.UiType = "cs";
                topologyGraph.setEdge(c.Source, c.Destination, c);
            });
        }
    }

    linkWithArrow(d) {
        var self = this;
        var pointCount = d.points.length;
        var 
            d90 = Math.PI / 2,
            sourceX = d.points[pointCount-2].x,
            sourceY = d.points[pointCount-2].y,
            targetX = d.points[pointCount-1].x,
            targetY = d.points[pointCount-1].y;

        var theta = Math.atan2(targetY - sourceY, targetX - sourceX);

        return self.line(d.points) +
            "M" + targetX + "," + targetY +
            "l" + (3.5 * Math.cos(d90 - theta) - 10 * Math.cos(theta)) + "," + (-3.5 * Math.sin(d90 - theta) - 10 * Math.sin(theta)) +
            "L" + (targetX - 3.5 * Math.cos(d90 - theta) - 10 * Math.cos(theta)) + "," + (targetY + 3.5 * Math.sin(d90 - theta) - 10 * Math.sin(theta)) + "z";
    }

    linkHasError(d: replicationTopologyConnectionDto) {
        return d.SourceToDestinationState !== "Online";
    }

    private iconText(rType: string) {
        switch (rType) {
            case "db":
                return "&#xf1c0";
            case "fs":
                return "&#xf1c5";
            case "cs":
                return "&#xf163";
            
            default:
                return "&#xf128";
        }
    }

    private getScaleFactorAndLeftOffset(containerWidth, graphWidth): { scaleFactor: number; leftOffset: number } {
        const extraPadding = 50;
        var scaleFactor = containerWidth / (graphWidth + extraPadding);
        if (scaleFactor > 1) {
            return {
                leftOffset: (containerWidth - graphWidth) / 2,
                scaleFactor: 1
            };
        } else {
            /*
            left offset is more complicated:
            - initially we compute scaleFactor with extra padding
            - multiply graphWidth by scaleFactor, it gives us width of graph
            - use: (containerWidth - graphWidth) / 2
            */
            var scaledGraphWidth = graphWidth * scaleFactor;
            var leftOffset = (containerWidth - scaledGraphWidth) / 2;
            return {
                leftOffset: leftOffset,
                scaleFactor: scaleFactor
            }
        }
    }

    renderTopology(topologyGraph) {
        var self = this;

        var graph = this.svg.selectAll('.graph').data([null]);

        var graphWidth = topologyGraph.graph().width;
        if (graphWidth === -Infinity) {
            graphWidth = 0;
        }

        this.dagreGraphSize = [topologyGraph.graph().width, topologyGraph.graph().height];

        var enteringGraph = graph.enter().append('g').attr('class', 'graph');
        var enteringGraphZoom = enteringGraph.append("g").attr("class", "graphZoom");

        enteringGraphZoom.append('g').attr('class', 'nodes');
        enteringGraphZoom.append('g').attr('class', 'edges');

        var { scaleFactor, leftOffset } = this.getScaleFactorAndLeftOffset(self.width, graphWidth);

        this.zoom.translate([leftOffset, 10]);
        this.zoom.scale(scaleFactor);

        graph
            .select('.graphZoom')
            .attr('transform', 'translate(' + leftOffset + ',10)scale(' + scaleFactor + ')');

        var mappedNodes = topologyGraph.nodes().map(n => topologyGraph.node(n));

        var nodesDom = this.svg.select('.nodes').selectAll('g.node').data(mappedNodes, d => d.id);

        nodesDom
            .exit()
            .transition()
            .style('opacity', 0)
            .remove();

        var nGroup = nodesDom.enter()
            .append('g')
            .attr('class', d =>  'node ' + d.rType)
            .attr('transform', d => 'translate(' + d.x + ',' + d.y + ')');

        nGroup.append('svg:rect')
            .attr('class', 'node')
            .attr('rx', 5)
            .attr("x", -110)
            .attr("y", -20)
            .attr('width', 220)
            .attr('height', 40);

        nGroup.append('svg:text')
            .attr("x", -100)
            .attr("y", 6)
            .attr('class', 'fa')
            .html(d => self.iconText(d.rType));

        nGroup.append('svg:text')
            .attr('x', 10)
            .attr('y', -4)
            .attr('class', 'id')
            .text(d => d.url);

        nGroup.append('svg:text')
            .attr('x', 10)
            .attr('y', 14)
            .attr('class', 'id')
            .text(d => d.name);

         nodesDom
            .transition()
            .attr('transform', d => 'translate(' + d.x + ',' + d.y + ')');

        var mappedEdges = topologyGraph.edges().map(e => topologyGraph.edge(e));

        var edgesDom = this.svg.select('.edges').selectAll('.link').data(mappedEdges, d => d.Source + "_" + d.Destination);

        edgesDom
            .exit()
            .transition()
            .style('opacity', 0)
            .remove();

        edgesDom.enter()
            .append('path')
            .attr('class', 'link')
            .classed('error', self.linkHasError)
            .attr('d', d => self.linkWithArrow(d))
            .on("click", function (d) {
                var currentSelection = d3.select(".selected").node();
                d3.selectAll(".selected").classed("selected", false);
                d3.select(this).classed('selected', currentSelection !== this);
                self.currentLink(currentSelection !== this ? d : null);
                self.documentToReplicateText(null);
            });

        edgesDom
            .transition()
            .attr('d', d => self.linkWithArrow(d));
    }

    fetchTopology() {
        this.showLoadingIndicator(true);
        new getGlobalReplicationTopology(this.fetchDb(), this.fetchFs(), this.fetchCs()) 
            .execute()
            .done((topo: globalTopologyDto) => {
                this.topology(topo);
                this.topologyFiltered(topo);
                this.createReplicationTopology();
            })
            .always(() => {
                this.currentLink(null);
                this.showLoadingIndicator(false);
            }); 
    }

    saveAsPng() {
        svgDownloader.downloadPng(d3.select('#replicationTopology').node(), 'replicationTopology.png', svg => {
            this.preprocesSvgDownload(svg);
            return topology.inlineCss;
        });
    }

    saveAsSvg() {
        svgDownloader.downloadSvg(d3.select('#replicationTopology').node(), 'replicationTopology.svg', (svg) => {
            this.preprocesSvgDownload(svg);
            return topology.inlineCss;
        });
    }

    preprocesSvgDownload(svg: Element) {
        this.removeIconsProcessor(svg);

        var padding = 10;

        $(svg)
            .attr('width', (this.dagreGraphSize[0] + 2 * padding)  + 'px')
            .attr('height', (this.dagreGraphSize[1]  + 2 * padding) + 'px');

        $(".graph", svg).attr('transform', 'translate(' + padding  + ',' + padding + ')');
        $(".graphZoom", svg).removeAttr('transform');
    }

    /*
    * Since we are using FontAwesome icons and they won't be avaialable in downloaded
    * svg, remove them from DOM
    * Also this procedure should fix issue with converting utf-8 data with window.atob.
    */
    removeIconsProcessor(svg: Element) {
        $("text.fa", svg).remove();
    }

    saveAsJson() {
        fileDownloader.downloadAsJson(this.topology(), "topology.json");
    }

    filter(criteria: string) {
        var filtered: globalTopologyDto = jQuery.extend(true, {}, this.topology());

        if (!criteria) {
            this.topologyFiltered(filtered);
            this.syncGraph();
            return;
        }

        if (filtered.Databases) {
            filtered.Databases.SkippedResources = filtered.Databases.SkippedResources.filter(s => s.indexOf(criteria) > -1);
            filtered.Databases.Servers = filtered.Databases.Servers.filter(s => s.indexOf(criteria) > -1);
            filtered.Databases.Connections = filtered.Databases.Connections.filter(s => s.Source.indexOf(criteria) > -1 && s.Destination.indexOf(criteria) > -1);
        }

        if (filtered.FileSystems) {
            filtered.FileSystems.SkippedResources = filtered.FileSystems.SkippedResources.filter(s => s.indexOf(criteria) > -1);
            filtered.FileSystems.Servers = filtered.FileSystems.Servers.filter(s => s.indexOf(criteria) > -1);
            filtered.FileSystems.Connections = filtered.FileSystems.Connections.filter(s => s.Source.indexOf(criteria) > -1 && s.Destination.indexOf(criteria) > -1);
        }

        if (filtered.Counters) {
            filtered.Counters.SkippedResources = filtered.Counters.SkippedResources.filter(s => s.indexOf(criteria) > -1);
            filtered.Counters.Servers = filtered.Counters.Servers.filter(s => s.indexOf(criteria) > -1);
            filtered.Counters.Connections = filtered.Counters.Connections.filter(s => s.Source.indexOf(criteria) > -1 && s.Destination.indexOf(criteria) > -1);
        }

        this.topologyFiltered(filtered);

        this.syncGraph();
    }

}

export = topology;
