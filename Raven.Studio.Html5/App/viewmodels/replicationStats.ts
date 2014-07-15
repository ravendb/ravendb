import viewModelBase = require("viewmodels/viewModelBase");
import getReplicationStatsCommand = require("commands/getReplicationStatsCommand");
import moment = require("moment");

import getReplicationTopology = require("commands/getReplicationTopology");

import d3 = require('d3/d3');

class replicationStats extends viewModelBase {

    topology = ko.observable<replicationTopologyDto>(null);
    currentLink = ko.observable<replicationTopologyLinkDto>(null);

    replStatsDoc = ko.observable<replicationStatsDocumentDto>();
    hasNoReplStatsAvailable = ko.observable(false);
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;

    nodes: any[];
    links: replicationTopologyLinkDto[];
    width: number;
    height: number;
    svg: D3.Selection;
    path: D3.UpdateSelection;
    circle: D3.UpdateSelection;
    force: D3.Layout.ForceLayout;
    colors = d3.scale.category10();

    constructor() {
        super();

        this.updateCurrentNowTime();
    }

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchReplStats());
        this.fetchReplStats();
    }

    attached() {
        this.resize();
        d3.select(window).on("resize", this.resize.bind(this));
    }

    resize() {
        this.width = $("#replicationTopologySection").width() * 0.6;
        if (this.topology() && !!this.force) {
            this.force.size([this.width, this.height]).resume();
        }
    }

    fetchReplStats() {
        this.replStatsDoc(null);
        new getReplicationStatsCommand(this.activeDatabase())
            .execute()
            .fail(() => this.hasNoReplStatsAvailable(true)) // If replication is not setup, the fetch will fail with 404.
            .done((result: replicationStatsDocumentDto) => {
                this.hasNoReplStatsAvailable(result.Stats.length === 0);
                this.processResults(result);
            });
    }

    processResults(results: replicationStatsDocumentDto) {
        if (results) {
            results.Stats.forEach(s => {
                s['LastReplicatedLastModifiedHumanized'] = this.createHumanReadableTime(s.LastReplicatedLastModified);
                s['LastFailureTimestampHumanized'] = this.createHumanReadableTime(s.LastFailureTimestamp);
                s['LastHeartbeatReceivedHumanized'] = this.createHumanReadableTime(s.LastHeartbeatReceived);
                s['LastSuccessTimestampHumanized'] = this.createHumanReadableTime(s.LastSuccessTimestamp);
                s['isHotFailure'] = this.isFailEarlierThanSuccess(s.LastFailureTimestamp, s.LastSuccessTimestamp);
            });
        }

        this.replStatsDoc(results);
    }

    createHumanReadableTime(time: string): KnockoutComputed<string> {
        if (time) {
            // Return a computed that returns a humanized string based off the current time, e.g. "7 minutes ago".
            // It's a computed so that it updates whenever we update this.now (scheduled to occur every minute.)
            return ko.computed(() => {
                var dateMoment = moment(time);
                var agoInMs = dateMoment.diff(this.now());
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            });
        }

        return ko.computed(() => time);
    }


    isFailEarlierThanSuccess(lastFailureTime: string, lastSuccessTime:string): boolean {
        if (!!lastFailureTime) {
            if (!!lastSuccessTime) {
                return lastFailureTime >= lastSuccessTime;
            } else {
                return true;
            }
        }

        return false;
    }

    updateCurrentNowTime() {
        this.now(moment());
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 60000);
    }

    createReplicationTopology() {
        var self = this;

        this.height = 600;

        this.svg = d3.select("#replicationTopology")
            .attr('width', self.width)
            .attr('height', self.height); 

        this.nodes = this.topology().Servers.map((s, idx) => {
            return { id: s, idx: idx + 1 }
        });

        var plainConnections: stringLinkDto[] = this.topology().Connections.map((connection:replicationTopologyConnectionDto) => {
            return {
                source: connection.Source,
                target: connection.Destination
            }
        });

        var masterMasterConnections: stringLinkDto[] = [];
        plainConnections.forEach((edge: stringLinkDto) => {
            var reverseConnection = plainConnections.first(c => c.source === edge.target && c.target === edge.source);
            if (reverseConnection) {
                // master - master connection!
                var reverseInMasterMasterCollection = !!masterMasterConnections.first(c => c.source === edge.target && c.target === edge.source);
                if (!reverseInMasterMasterCollection) {
                    masterMasterConnections.push(edge);
                }
            }
        });

        this.links = this.topology().Connections
            .filter(connection => masterMasterConnections.first(mc => connection.Source == mc.source && connection.Destination == mc.target) == null)
            .map((connection: replicationTopologyConnectionDto) => {
                // find complementary connection in master-master connections
                var masterMasterExists = !!masterMasterConnections.first(mc => connection.Source == mc.target && connection.Destination == mc.source);

                var masterMaster = masterMasterExists
                    ? this.topology().Connections.first(c => c.Source == connection.Destination && c.Destination == connection.Source)
                    : null;

             return { 
                 source: self.nodes.first(e => e.id === connection.Source),
                 target: self.nodes.first(e => e.id === connection.Destination),
                 left: masterMasterExists,
                 right: true,
                 toRightPayload: connection,
                 toLeftPayload: masterMaster
            }
        });

        var self = this;
        this.force = d3.layout.force()
            .nodes(this.nodes)
            .links(<any>this.links)
            .size([this.width, this.height])
            .linkDistance(220)
            .charge(-2500)
            .on('tick', self.tick.bind(self));

        this.defineArrowMarkers();

        this.path = <D3.UpdateSelection>this.svg.append('svg:g').selectAll('path');
        this.circle = <D3.UpdateSelection>this.svg.append('svg:g').selectAll('g');

        this.restart();

    }

    tick() {
        var boxWidth = 200;
        var boxHeight = 40;
        // draw directed edges with proper padding from node centers
        this.path.attr('d', function (d) {

            var deltaX = d.target.x - d.source.x,
                deltaY = d.target.y - d.source.y;

            var alpha = Math.abs(Math.atan(deltaY / deltaX));
            var edgeAlpha = Math.atan(boxHeight / boxWidth);
            var leftRightCase = alpha < edgeAlpha;
            var distance = 0;
            if (leftRightCase) {
                distance = boxWidth / (2 * Math.cos(alpha));
            } else {
                distance = boxHeight / (2 * Math.sin(alpha));
            }

            var dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
                normX = deltaX / dist,
                normY = deltaY / dist,
                sourcePadding =  distance + (d.left ? 5 : 0),
                targetPadding = distance + (d.right ? 5 : 0),
                sourceX = d.source.x + (sourcePadding * normX),
                sourceY = d.source.y + (sourcePadding * normY),
                targetX = d.target.x - (targetPadding * normX),
                targetY = d.target.y - (targetPadding * normY);
            return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
        });
         
        this.circle.attr('transform', function (d) {
            return 'translate(' + d.x + ',' + d.y + ')';
        });
    }

    linkHasError(d: replicationTopologyLinkDto) {
        return d.left && d.toLeftPayload.SourceToDestinationState !== "Online"
            || d.right && d.toRightPayload.SourceToDestinationState !== "Online";
    }

    restart() {
        var self = this;
        // path (link) group
        this.path = this.path.data(this.links);

        // add new links
        this.path.enter()
            .append('svg:path')
            .attr('class', 'link')
            .classed('error', self.linkHasError)
            .style('marker-start', (d: replicationTopologyLinkDto) => {
                if (!d.left) {
                    return '';
                }
                return self.linkHasError(d) ? 'url(#start-arrow-red)' : 'url(#start-arrow-green)';
            })
            .style('marker-end', (d: replicationTopologyLinkDto) => {
                if (!d.right) {
                    return '';
                }
                return self.linkHasError(d) ? 'url(#end-arrow-red)' : 'url(#end-arrow-green)';
            })
            .on("click", function (d) {
                var currentSelection = d3.select(".selected").node();
                d3.selectAll(".selected").classed("selected", false); 
                d3.select(this).classed('selected', currentSelection != this);
                self.currentLink( currentSelection != this ? d : null)
             });

        // remove old links
        this.path.exit().remove();
        
        // circle (node) group
        this.circle = this.circle.data(self.nodes, function (d) { return d.id; });

        // add new nodes
        var g = this.circle.enter().append('svg:g').call(self.force.drag);

        g.append('svg:rect')
            .attr('class', 'node')
            .attr('rx', 5)
            .attr("x", -100)
            .attr("y", -20)
            .attr('width', 200)
            .attr('height', 40);

        // show node IDs
        g.append('svg:text')
            .attr('x', 0)
            .attr('y', -4)
            .attr('class', 'id')
            .text(d => d.id.split("/databases/")[0]);

        g.append('svg:text')
            .attr('x', 0)
            .attr('y', 14)
            .attr('class', 'id')
            .text(d => d.id.split("/databases/")[1]);

        // remove old nodes
        this.circle.exit().remove();

        // set the graph in motion
        this.force.start();
    }


    defineArrowMarkers() {

        // define arrow markers for graph links
        this.svg.append('svg:defs').append('svg:marker')
            .attr('id', 'end-arrow-red')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 6)
            .attr('markerWidth', 3)
            .attr('markerHeight', 3)
            .attr('orient', 'auto')
            .append('svg:path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', 'red');

        this.svg.append('svg:defs').append('svg:marker')
            .attr('id', 'start-arrow-red')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 4)
            .attr('markerWidth', 3)
            .attr('markerHeight', 3)
            .attr('orient', 'auto')
            .append('svg:path')
            .attr('d', 'M10,-5L0,0L10,5')
            .attr('fill', 'red');

        this.svg.append('svg:defs').append('svg:marker')
            .attr('id', 'end-arrow-green')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 6)
            .attr('markerWidth', 3)
            .attr('markerHeight', 3)
            .attr('orient', 'auto')
            .append('svg:path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', 'green');

        this.svg.append('svg:defs').append('svg:marker')
            .attr('id', 'start-arrow-green')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 4)
            .attr('markerWidth', 3)
            .attr('markerHeight', 3)
            .attr('orient', 'auto')
            .append('svg:path')
            .attr('d', 'M10,-5L0,0L10,5')
            .attr('fill', 'green');
    }

    fetchTopology() {
        new getReplicationTopology(this.activeDatabase())
            .execute()
            .done((topo) => {
                this.topology(topo); 
                this.createReplicationTopology();
            }); 
    }

}

export = replicationStats;