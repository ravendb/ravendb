import lineChart = require("models/resources/clusterDashboard/lineChart");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import indexingSpeed = require("models/resources/widgets/indexingSpeed");
import abstractChartsWebsocketWidget = require("viewmodels/resources/widgets/abstractChartsWebsocketWidget");

class indexingWidget extends abstractChartsWebsocketWidget<Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload, indexingSpeed> {

    indexedPerSecondChart: lineChart;
    mappedPerSecondChart: lineChart;
    reducedPerSecondChart: lineChart;
    
    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new indexingSpeed(node.tag());
            this.nodeStats.push(stats);
        }
    }
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "Indexing";
    }

    initCharts() {
        const indexedPerSecondContainer = this.container.querySelector(".indexed-per-second-chart");
        this.indexedPerSecondChart = new lineChart(indexedPerSecondContainer, {
            grid: true,
            tooltipProvider: date => indexingWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        const mappedPerSecondContainer = this.container.querySelector(".mapped-per-second-chart");
        this.mappedPerSecondChart = new lineChart(mappedPerSecondContainer, {
            grid: true,
            tooltipProvider: date => indexingWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        const reducedPerSecondContainer = this.container.querySelector(".reduced-per-second-chart");
        this.reducedPerSecondChart = new lineChart(reducedPerSecondContainer, {
            grid: true,
            tooltipProvider: date => indexingWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        
        return [this.indexedPerSecondChart, this.mappedPerSecondChart, this.reducedPerSecondChart];
    }

    protected extractDataForChart(chart: lineChart, data: Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload): number {
        if (chart === this.indexedPerSecondChart) {
            return data.IndexedPerSecond;
        } else if (chart === this.mappedPerSecondChart) {
            return data.MappedPerSecond;
        } else if (chart === this.reducedPerSecondChart) {
            return data.ReducedPerSecond;
        } else {
            throw new Error("Unsupported chart: " + chart);
        }
    }
}

export = indexingWidget;
