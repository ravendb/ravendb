import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import abstractChartsWebsocketWidget = require("viewmodels/resources/widgets/abstractChartsWebsocketWidget");
import { lineChart } from "models/resources/clusterDashboard/lineChart";
import serverTraffic = require("models/resources/widgets/serverTraffic");

interface trafficState {
    showWritesDetails: boolean;
    showDataWrittenDetails: boolean;
}

class trafficWidget extends abstractChartsWebsocketWidget<Raven.Server.Dashboard.Cluster.Notifications.TrafficWatchPayload, serverTraffic, void, trafficState> {

    view = require("views/resources/widgets/trafficWidget.html");
    
    showWritesDetails = ko.observable<boolean>(false);
    showDataWrittenDetails = ko.observable<boolean>(false);
    
    requestsChart: lineChart;
    avgRequestTimeChart: lineChart;
    writesChart: lineChart;
    dataWrittenChart: lineChart;
    
    constructor(controller: clusterDashboard) {
        super(controller);
        
        _.bindAll(this, "toggleWritesDetails", "toggleDataWrittenDetails");

        for (const node of this.controller.nodes()) {
            const stats = new serverTraffic(node.tag());
            this.nodeStats.push(stats);
        }
    }
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "Traffic";
    }

    getState(): trafficState {
        return {
            showDataWrittenDetails: this.showDataWrittenDetails(),
            showWritesDetails: this.showWritesDetails()
        }
    }

    restoreState(state: trafficState) {
        this.showWritesDetails(state.showWritesDetails);
        this.showDataWrittenDetails(state.showDataWrittenDetails);
    }
    
    initCharts() {
        const requestsContainer = this.container.querySelector(".requests-chart");
        this.requestsChart = new lineChart(requestsContainer, {
            grid: true,
            fillData: true,
            tooltipProvider: date => trafficWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });

        const avgRequestTimeContainer = this.container.querySelector(".avg-request-time-chart");
        this.avgRequestTimeChart = new lineChart(avgRequestTimeContainer, {
            grid: true,
            fillData: true,
            tooltipProvider: date => trafficWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        
        const writesChartContainer = this.container.querySelector(".writes-chart");
        this.writesChart = new lineChart(writesChartContainer, {
            grid: true,
            fillData: true,
            tooltipProvider: date => trafficWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        
        const dataWrittenContainer = this.container.querySelector(".data-written-chart");
        this.dataWrittenChart = new lineChart(dataWrittenContainer, {
            grid: true,
            fillData: true,
            tooltipProvider: date => trafficWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        
        return [this.requestsChart, this.avgRequestTimeChart, this.writesChart, this.dataWrittenChart];
    }

    protected extractDataForChart(chart: lineChart, data: Raven.Server.Dashboard.Cluster.Notifications.TrafficWatchPayload): number {
        if (chart === this.requestsChart) {
            return data.RequestsPerSecond;
        } else if (chart === this.avgRequestTimeChart) {
            return data.AverageRequestDuration;
        } else if (chart === this.writesChart) {
            return data.DocumentWritesPerSecond + data.AttachmentWritesPerSecond + data.CounterWritesPerSecond + data.TimeSeriesWritesPerSecond;
        } else if (chart === this.dataWrittenChart) {
            return data.DocumentsWriteBytesPerSecond + data.AttachmentsWriteBytesPerSecond + data.CountersWriteBytesPerSecond + data.TimeSeriesWriteBytesPerSecond;
        } else {
            throw new Error("Unsupported chart: " + chart);
        }
    }
    
    toggleWritesDetails() {
        this.showWritesDetails.toggle();

        this.controller.layout(true, "shift");
    }

    toggleDataWrittenDetails() {
        this.showDataWrittenDetails.toggle();

        this.controller.layout(true, "shift");
    }
}

export = trafficWidget;
