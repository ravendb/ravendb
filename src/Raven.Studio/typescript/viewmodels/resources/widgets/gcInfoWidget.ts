import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import gcInfo = require("models/resources/widgets/gcInfo");
import abstractTransformingChartsWebsocketWidget
    from "viewmodels/resources/widgets/abstractTransformingChartsWebsocketWidget";
import { lineChart } from "models/resources/clusterDashboard/lineChart";
import moment = require("moment");
import { clusterDashboardChart } from "models/resources/clusterDashboard/clusterDashboardChart";
import { bubbleChart } from "models/resources/clusterDashboard/bubbleChart";

interface gcInfoState {
    showGenerationsDetails: boolean;
    showPausesDetails: boolean;
}

class gcInfoWidget extends abstractTransformingChartsWebsocketWidget<
    Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload, 
    GcInfoNormalizedData,
    GcInfoChartData,
    gcInfo,
    void,
    gcInfoState
> {
    view = require("views/resources/widgets/gcInfoWidget.html");

    showGenerationsDetails = ko.observable<boolean>(false);
    showPausesDetails = ko.observable<boolean>(false);
    
    generationsSizeCharts: lineChart<GcInfoChartData>[] = [];
    pausesChart: bubbleChart<GcInfoChartData>;
    
    private readonly nodeTags: string[] = [];
    
    constructor(controller: clusterDashboard) {
        super(controller);
        
        for (const node of this.controller.nodes()) {
            const stats = new gcInfo(node.tag());
            this.nodeStats.push(stats);
            this.nodeTags.push(node.tag());
        }
    }
    
    gcIndexCache = new Map<string, number>();
    
    static gcTypes = ["Ephemeral", "FullBlocking", "Background"] as const;

    alreadySeenNodeTags = new Set<string>();
    
    transformSocketData(nodeTag: string, input: Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload): GcInfoNormalizedData[] {
        if (!this.alreadySeenNodeTags.has(nodeTag)) {
            this.alreadySeenNodeTags.add(nodeTag);
            return [];
        }
        
        const base: Pick<GcInfoNormalizedData, "Date" | "Type"> = {
            Date: input.Date,
            Type: input.Type
        };
        
        const output: GcInfoNormalizedData[] = [];
        
        const dateParsed = moment.utc(input.Date).toDate();
        
        let doHeartbeat = false;
        
        gcInfoWidget.gcTypes.forEach(gcType => {
            const cacheKey = nodeTag + "_" + gcType;
            const lastKnownIndex = this.gcIndexCache.get(cacheKey) ?? -1;
            const memoryInfo = input[gcType];
            if (memoryInfo) {
                const incomingIndex = memoryInfo.Index;
                if (incomingIndex !== lastKnownIndex) {
                    // it covers two cases:
                    // incomingIndex > lastKnownIndex ==> there was new GC 
                    // incomingIndex < lastKnownIndex ==> looks like server was restarted?

                    output.push({
                        ...base,
                        gcType,
                        memoryInfo,
                    })
                }

                this.gcIndexCache.set(cacheKey, incomingIndex);
            } else {
                doHeartbeat = true;
            }
        });

        if (doHeartbeat) {
            this.charts.forEach(x => x.onHeartbeat(dateParsed));
        }
        
        // since we query for GC info in intervals there might be multiple GCs in same polling period
        // sort by GC index to draw graphs properly
        return _.sortBy(output, x => x.memoryInfo.Index);
    }
    
    transformChartData(key: string, item: GcInfoNormalizedData): GcInfoChartData[] {
        const beforeDate = moment.utc(item.Date).add(-1, "milliseconds").toISOString();
        const afterDate = item.Date;
        
        const accessors: Array<{
            accessor: (item: Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GcMemoryInfo) => Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GenerationInfoSize,
            key: string
        }>
            = [
            {
                accessor: item => item.LargeObjectHeapSize,
                key: "gc-info-loh"
            }, {
                accessor: item => item.PinnedObjectHeapSize,
                key: "gc-info-pinned"
            }, {
                accessor: item => item.Gen2HeapSize,
                key: "gc-info-gen2"
            }, {
                accessor: item => item.Gen1HeapSize,
                key: "gc-info-gen1"
            }, {
                accessor: item => item.Gen0HeapSize,
                key: "gc-info-gen0"
            }
        ];
        
        const output: GcInfoChartData[] = [];
        
        const extractedValuesBefore = accessors.map(x => x.accessor(item.memoryInfo).SizeBeforeBytes);
        const extractedValuesAfter = accessors.map(x => x.accessor(item.memoryInfo).SizeAfterBytes);
        
        for (let i = 0; i < accessors.length; i++) {
            const partialSumBefore = extractedValuesBefore.slice(0, i + 1).reduce((sum, val) => sum + val, 0);
            output.push({
                Key: accessors[i].key,
                Date: beforeDate,
                value: partialSumBefore,
                gcType: item.gcType,
            });

            const partialSumAfter = extractedValuesAfter.slice(0, i + 1).reduce((sum, val) => sum + val, 0);
            output.push({
                Key: accessors[i].key,
                Date: afterDate,
                value: partialSumAfter,
                gcType: item.gcType,
            });
        }
        
        output.push({
            Key: gcInfoWidget.pausesPrefix + key,
            Date: item.Date,
            value: item.memoryInfo.PauseDurationsInMs.reduce((p, c) => p + c, 0),
            gcType: item.gcType,
        })
        
        return output;
    }
    
    static readonly pausesPrefix = "node-";

    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "GcInfo";
    }

    getState(): gcInfoState {
        return {
            showGenerationsDetails: this.showGenerationsDetails(),
            showPausesDetails: this.showPausesDetails(),
        }
    }

    restoreState(state: gcInfoState) {
        this.showGenerationsDetails(state.showGenerationsDetails);
        this.showPausesDetails(state.showPausesDetails);
    }

    protected canAppendToChart(chart: clusterDashboardChart<GcInfoChartData>, nodeTag: string, item: GcInfoChartData): boolean {
        if (item.Key.startsWith(gcInfoWidget.pausesPrefix)) {
            return chart instanceof bubbleChart;
        }
        
        const chartIdx = this.charts.findIndex(x => x === chart);
        const keyIdx = this.nodeTags.findIndex(x => x === nodeTag);
        return chartIdx === keyIdx;
    }

    protected initCharts() {
        const generationsSizeChartsContainer = this.container.querySelector(".graph-containers");

        const charts = this.nodeTags.map(node => {
            const div = document.createElement("div");
            div.setAttribute("class", "graph-container gc-size-line-chart node-" + node);
            
            generationsSizeChartsContainer.appendChild(div);
            
            return new lineChart<GcInfoChartData>(div,
                x => x.value,
                {
                    grid: true,
                    fillArea: true,
                    topPaddingProvider: () => 2,
                    onMouseMove: date => this.mouseMovedLineChart(date, node)
                });
        });
        
        this.generationsSizeCharts = charts;

        const pausesChartContainer = this.container.querySelector(".gc-pause-bubble-chart");
        this.pausesChart = new bubbleChart<GcInfoChartData, {  gcType: string }>(pausesChartContainer, x => x.value, {
            grid: true,
            topPaddingProvider: () => 8,
            onMouseMove: date => this.mouseMovedPausesChart(date),
            extraArgumentsProvider: payload => ({ gcType: payload.gcType }),
            drawPoints: (selection) => {
                selection
                    .append("circle")
                    .attr("r", 8);
                
                selection.append("text")
                    .attr("y", 4)
                    .text(d => d.gcType.substring(0, 1));
            }
        });
        return [...charts, this.pausesChart];
    }

    mouseMovedPausesChart(date: Date | null) {
        const closestItem = this.findClosestItem(date);
        if (closestItem) {
            // here we on purpose ignore closestItem date -> to detect no data, we only care here about node tag
            this.mouseMovedLineChart(date, closestItem.tag);    
        } else {
            this.charts.forEach(x => x.highlightTime(null));
            this.showNodesHistory(null, true);
        }
    }
    
    private findClosestItem(date: Date | null) {
        if (!date) {
            return null;
        }
        // find the closest date on all tracks and call mouseMovedLineChart
        const perNodeData = this.nodeStats().map(nodeStats => {
            const dates = nodeStats.history.map(x => x.date);
            const alignedDate = this.quantizeDate(date, dates);
            return { date: alignedDate, tag: nodeStats.tag, distance: alignedDate ? Math.abs(date.getTime() - alignedDate.getTime()) : Number.MAX_SAFE_INTEGER };
        });

        const sortedPerNodeData = _.sortBy(perNodeData, x => x.distance, "asc");
        return sortedPerNodeData[0];
    }
    
    
    mouseMovedLineChart(date: Date | null, nodeTag: string) {
        const nodeStats = this.nodeStats().find(x => x.tag === nodeTag);
        const dates = nodeStats.history.map(x => x.date);
        const alignedDate = this.quantizeDate(date, dates);
        
        this.nodeTags.forEach((currentNodeTag, index) => {
            if (currentNodeTag === nodeTag) {
                this.charts[index].highlightTime(alignedDate);
            } else {
                this.charts[index].highlightTime(null);
            }
        });
        
        this.pausesChart.highlightTime(alignedDate);
        
        this.showNodesHistory(alignedDate, !date);
    }

    toggleGenerationsDetails() {
        this.showGenerationsDetails.toggle();

        this.controller.layout(true, "shift");
    }
    
    togglePausesDetails() {
        this.showPausesDetails.toggle();
        
        this.controller.layout(true, "shift");
    }
}

export = gcInfoWidget;
