import getUserInfoCommand = require("commands/getUserInfoCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import d3 = require('d3/d3');
import queryIndexDebugMapCommand = require("commands/queryIndexDebugMapCommand");
import queryIndexDebugReduceCommand = require("commands/queryIndexDebugReduceCommand");

class visualizer extends viewModelBase {

    keys = ko.observableArray<string>();

    indexName = ko.observable("ByCounty");

    nodes: any = [];
    links: any = [];
    noForceLinks: any = [];
    width = 1300;
    height = 600;

    node = null;
    link = null;
    noForceLink = null;
    fill = null;
    force = null;

    inputDocs:any = {}; // map source id -> chart node
    inputDocsSettings = {
        targetY: 100,
        padding: 100,
        counter: 0
    }
    mappedDocsSettings = {
        targetY: 250,
    }
    reducedDocsLevel1Settings = {
        targetY: 450,
        padding: 150,
        counter: 0
    }
    reducedDocsLevel2Settings = {
        targetY: 550,
        padding: 150,
        counter: 0
    }

    attached() {
        this.fill = d3.scale.category10();

        var svg = d3.select("#visualizer")
            .attr("width", this.width)
            .attr("height", this.height);

        this.force = d3.layout.force()
            .nodes(this.nodes)
            .links(this.links)
            .linkDistance((d, idx) => {
                if (d.source.type == 1 && d.target.type == 2) {
                    return this.mappedDocsSettings.targetY - this.inputDocsSettings.targetY;
                } else if (d.source.type == 2 && d.target.type == 3) {
                    return this.reducedDocsLevel1Settings.targetY - this.mappedDocsSettings.targetY;
                }
                return 20;
            })
            .linkStrength((d, idx) => {
                if (d.source.type == 2 && d.target.type == 3) {
                    return 0.5;
                } 
                return 1;
            })
            .gravity(0)
            .size([this.width, this.height])
            .on("tick", this.tick.bind(this));

        this.node = svg.selectAll("circle");
        this.link = svg.selectAll("line.link");
        this.noForceLink = svg.selectAll("line.noForceLink");
    }


    addItem() {
        var key = $("#itemKey").val();
        //TODO: duplicate detection
        this.keys.push(key);
        $("#itemKey").val("");

        this.fetchDataFor(key).then(() => this.updateGraph());

    }
    enterFullscreen() {
        if (
            document.fullscreenEnabled ||
            document.webkitFullscreenEnabled ||
            document.mozFullScreenEnabled ||
            document.msFullscreenEnabled
            ) {
            var container:any = document.getElementById("visualizerContainer");

            // go full-screen
            if (container.requestFullscreen) {
                container.requestFullscreen();
            } else if (container.webkitRequestFullscreen) {
                container.webkitRequestFullscreen();
            } else if (container.mozRequestFullScreen) {
                container.mozRequestFullScreen();
            } else if (container.msRequestFullscreen) {
                container.msRequestFullscreen();
            }
        } else {
            console.log("not supported");
             //TODO: we don't support this feature!
        }

    }

    updateGraph() {
        var self = this;
        this.force.start();
        this.node = this.node.data(this.nodes);
        this.link = this.link.data(this.links);
        this.noForceLink = this.noForceLink.data(this.noForceLinks);

        (<any>this.link).enter().append("line").attr("class", "link");
        (<any>this.noForceLink).enter().append("line").attr("class", "noForceLink");

        var enteringNodes = (<any>this.node).enter().append("g").attr("class", "node")

        enteringNodes.append("circle")
            .attr("r", 8)
            .style("fill", d => self.fill(d.type))
            .style("stroke", function (d) { return d3.rgb(self.fill(d.type)).darker(2); })
            .on("mouseover", function () {
                console.log(d3.select(this));
            })
            .call(this.force.drag);

        enteringNodes.append("text")
            .attr("x", 12)
            .attr("y", ".35em")
            .attr("text-anchor", "middle")
            .text(d => d.id);

        //TODO: d3.select.exit()
    }

    fetchDataFor(key: string) {
        var allDataFetched = $.Deferred();

        var mapTask = new queryIndexDebugMapCommand(this.indexName(), this.activeDatabase(), key, 0, 256)
            .execute().then(results => {

                return results.map(result => {
                    var sourceNode = this.getOrCreateInputDocNode(result.Source);
                    var mappedDocNode = this.createMappedDocNode(result);

                    this.links.push({
                        source: sourceNode,
                        target: mappedDocNode
                    });
                    return mappedDocNode;
                });
            });

        var reduce1Task = mapTask.then((mapResults) => {
            //TODO: support for counter
            return new queryIndexDebugReduceCommand(this.indexName(), this.activeDatabase(), 1, key, 0, 256)
                .execute().then(results => {
                    var resultsByBucket = {};

                    results.forEach(result => {
                        var node = this.createReduce1Node(result);
                        resultsByBucket[result.Source] = node;
                    });

                    mapResults.forEach(r => {
                        this.noForceLinks.push({
                            source: r,
                            target: resultsByBucket[String(r.bucket)]
                        });
                    });
                });
        })

        var reduce2Task = reduce1Task.then(() => {
            return new queryIndexDebugReduceCommand(this.indexName(), this.activeDatabase(), 2, key, 0, 256)
                .execute().then(results => {
                    results.forEach(result => {
                        var r2Node = this.createReduce2Node(result);
                    });
                });
        }).then(() => allDataFetched.resolve());

        return allDataFetched;
    }

    getOrCreateInputDocNode(source: string) {
        if (source in this.inputDocs) {
            return this.inputDocs[source];
        }

        var node = {
            x: Math.random() * this.width,
            y: this.height - 50,
            type: 1,
            id: source,
            idx: this.inputDocsSettings.counter++
        };
        this.inputDocs[source] = node;
        this.nodes.push(node);
        return node;
    }

    createMappedDocNode(result: mappedResultInfo) {
        var node = {
            x: Math.random() * this.width,
            y: this.height - 100,
            type: 2,
            id: result.ReduceKey,
            source: result.Source,
            bucket: result.Bucket
        };

        this.nodes.push(node);
        return node;
    }

    createReduce1Node(result: mappedResultInfo) {
        var node = {
            x: Math.random() * this.width,
            y: this.height,
            type: 3,
            id: result.ReduceKey,
            source: result.Source,
            idx: this.reducedDocsLevel1Settings.counter++
        }

        this.nodes.push(node);
        return node; 
    }

    createReduce2Node(result: mappedResultInfo) {
        var node = {
            x: Math.random() * this.width,
            y: this.height,
            type: 4,
            id: result.ReduceKey,
            source: result.Source,
            idx: this.reducedDocsLevel2Settings.counter++
        }

        this.nodes.push(node);
        return node;
    }

    tick(e) {

        var k = .1 * e.alpha;
        var self = this;
        this.nodes.forEach(function (o, i) {
            if (!o.fixed) {
                var targetPosition = self.computeTargetPosition(o);
                o.x += (targetPosition.x - o.x) * k;
                o.y += (targetPosition.y - o.y) * k;
            }
        });

        this.node.select("circle")
            .attr("cx", function (d) { return d.x; })
            .attr("cy", function (d) { return d.y; });

        this.node.select("text")
                    .attr("x", function (d) { return d.x; })
                    .attr("y", function (d) { return d.y  - 10; });

        this.link.attr("x1", function (d) { return d.source.x; })
            .attr("y1", function (d) { return d.source.y; })
            .attr("x2", function (d) { return d.target.x; })
            .attr("y2", function (d) { return d.target.y; });

        this.noForceLink.attr("x1", function (d) { return d.source.x; })
            .attr("y1", function (d) { return d.source.y; })
            .attr("x2", function (d) { return d.target.x; })
            .attr("y2", function (d) { return d.target.y; });
    }

    interpolate(idx: number, total: number, padding: number, totalWidth: number) {
        if (total == 1) {
            return totalWidth / 2;
        }
        return padding + (idx * (totalWidth - 2 * padding) / (total - 1));
    }

    computeTargetPosition(o: visualizerDataObjectDto) {
        if (o.type === 1) {
            return {
                x: this.interpolate(o.idx, this.inputDocsSettings.counter, this.inputDocsSettings.padding, this.width),
                y: this.inputDocsSettings.targetY + (o.idx % 4) * 20
            };
        } else if (o.type === 2) {
            var idx = <number>this.inputDocs[o.source].idx;
            return {
                x: this.interpolate(idx, this.inputDocsSettings.counter, this.inputDocsSettings.padding, this.width),
                y: this.mappedDocsSettings.targetY + (idx % 4) * 20
            };
        } else if (o.type === 3) {
            return {
                x: this.interpolate(o.idx, this.reducedDocsLevel1Settings.counter, this.reducedDocsLevel1Settings.padding, this.width),
                y: this.reducedDocsLevel1Settings.targetY
            };
        } else if (o.type === 4) {
            return {
                x: this.interpolate(o.idx, this.reducedDocsLevel2Settings.counter, this.reducedDocsLevel2Settings.padding, this.width),
                y: this.reducedDocsLevel2Settings.targetY
            };
        }

        else {
            return {
                x: o.x,
                y: o.y
            }
        }
    }

}


export = visualizer;