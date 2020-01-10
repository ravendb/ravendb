import document = require("models/database/documents/document");

type timeSeriesItem = {
    document: document;
    path: string;
}

type viewMode = "plot" | "table";

class timeSeriesChart {
    
    private mode = ko.observable<viewMode>();
    private series = ko.observableArray<timeSeriesItem>([]);
    
    constructor(series: Array<timeSeriesItem>, initialMode: viewMode = "plot") {
        this.series(series);
        this.mode(initialMode);
    }
 
    //TODO: plot graph
}

export = timeSeriesChart;
