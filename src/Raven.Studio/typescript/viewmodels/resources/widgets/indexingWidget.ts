import { lineChart } from "models/resources/clusterDashboard/lineChart";
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import indexingSpeed = require("models/resources/widgets/indexingSpeed");
import abstractChartsWebsocketWidget = require("viewmodels/resources/widgets/abstractChartsWebsocketWidget");

class indexingWidget extends abstractChartsWebsocketWidget<Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload, indexingSpeed> {

    view = require("views/resources/widgets/indexingWidget.html");
    
    indexedPerSecondChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload>;
    mappedPerSecondChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload>;
    reducedPerSecondChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload>;
    
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
        this.indexedPerSecondChart = new lineChart(indexedPerSecondContainer, x => x.IndexedPerSecond, {
            grid: true,
            tooltipProvider: date => indexingWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        const mappedPerSecondContainer = this.container.querySelector(".mapped-per-second-chart");
        this.mappedPerSecondChart = new lineChart(mappedPerSecondContainer, x => x.MappedPerSecond, {
            grid: true,
            tooltipProvider: date => indexingWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        const reducedPerSecondContainer = this.container.querySelector(".reduced-per-second-chart");
        this.reducedPerSecondChart = new lineChart(reducedPerSecondContainer, x => x.ReducedPerSecond, {
            grid: true,
            tooltipProvider: date => indexingWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        
        return [this.indexedPerSecondChart, this.mappedPerSecondChart, this.reducedPerSecondChart];
    }
}

export = indexingWidget;
