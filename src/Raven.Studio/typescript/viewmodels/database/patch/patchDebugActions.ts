import { highlight, languages } from "prismjs";


class patchDebugActions {
    loadDocument = ko.observableArray<any>();
    putDocument = ko.observableArray<any>();
    deleteDocument = ko.observableArray<string>();
    getCounter = ko.observableArray<any>();
    incrementCounter = ko.observableArray<any>();
    deleteCounter = ko.observableArray<string>();
    getTimeSeries = ko.observableArray<any>();
    appendTimeSeries = ko.observableArray<any>();
    deleteTimeSeries = ko.observableArray<any>();

    loadedCount: KnockoutComputed<string>;
    modifiedCount: KnockoutComputed<string>;
    deletedCount: KnockoutComputed<string>;

    showDocumentsInModified = ko.observable<boolean>(false);
    showTimeSeriesValuesInLoaded = ko.observable<boolean>(false);
    showTimeSeriesValuesInModified = ko.observable<boolean>(false);
    
    constructor() {
        this.initObservables();
    }

    formatAsJson(input: KnockoutObservable<any> | any) {
        return ko.pureComputed(() => {
            const value = ko.unwrap(input);
            if (value === undefined) {
                return "";
            } else {
                const json = JSON.stringify(value, null, 4);
                return highlight(json, languages.javascript, "js");
            }
        });
    }
    
    private initObservables() {
        this.loadedCount = ko.pureComputed(() => {
            const totalLoadedCount = this.loadDocument().length +
                this.getCounter().length +
                this.getTimeSeries().length;

            return totalLoadedCount ? totalLoadedCount.toLocaleString() : "";
        });

        this.modifiedCount = ko.pureComputed(() => {
            const totalModifiedCount = this.putDocument().length +
                this.incrementCounter().length +
                this.appendTimeSeries().length;

            return totalModifiedCount ? totalModifiedCount.toLocaleString() : "";
        });

        this.deletedCount = ko.pureComputed(() => {
            const totalDeletedCount = this.deleteDocument().length +
                this.deleteCounter().length +
                this.deleteTimeSeries().length;

            return totalDeletedCount ? totalDeletedCount.toLocaleString() : "";
        });
    }
    
    fill(incomingDto: Raven.Server.Documents.Patch.PatchDebugActions) {
        this.loadDocument(incomingDto.LoadDocument);
        this.putDocument(incomingDto.PutDocument);
        this.deleteDocument(incomingDto.DeleteDocument);

        this.getCounter(incomingDto.GetCounter);
        this.incrementCounter(incomingDto.IncrementCounter);
        this.deleteCounter(incomingDto.DeleteCounter);

        this.getTimeSeries(incomingDto.GetTimeSeries);
        this.appendTimeSeries(incomingDto.AppendTimeSeries);
        this.deleteTimeSeries(incomingDto.DeleteTimeSeries);
    }
    
    reset() {
        this.loadDocument([]);
        this.putDocument([]);
        this.deleteDocument([]);

        this.getCounter([]);
        this.incrementCounter([]);
        this.deleteCounter([]);

        this.getTimeSeries([]);
        this.appendTimeSeries([]);
        this.deleteTimeSeries([]);
    }
    
}

export = patchDebugActions;
