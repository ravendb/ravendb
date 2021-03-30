import generalUtils = require("common/generalUtils");


class indexingSpeed {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);

    hasData = ko.observable<boolean>(false);

    indexedPerSecond = ko.observable<number>(0);
    mappedPerSecond = ko.observable<number>(0);
    reducedPerSecond = ko.observable<number>(0);

    indexedPerSecondFormatted: KnockoutComputed<string>;
    mappedPerSecondFormatted: KnockoutComputed<string>;
    reducedPerSecondFormatted: KnockoutComputed<string>;
    
    constructor(tag: string) {
        this.tag = tag;
        
        this.indexedPerSecondFormatted = this.createObservable(() => this.indexedPerSecond());
        this.mappedPerSecondFormatted = this.createObservable(() => this.mappedPerSecond());
        this.reducedPerSecondFormatted = this.createObservable(() => this.reducedPerSecond());
    }
    
    private createObservable(valueProvider: () => number) {
        return ko.pureComputed(() => {
            if (this.disconnected() || !this.hasData()) {
                return "Connecting...";
            }
            const value = valueProvider();
            if (value < 0.001) {
                return "0";
            }
            if (value < 1) {
                return generalUtils.formatNumberToStringFixed(value, 2);
            }
            return generalUtils.formatNumberToStringFixed(value, 0);
        });
    }

    update(data: Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload) {
        this.hasData(true);
        
        this.mappedPerSecond(data.MappedPerSecond);
        this.reducedPerSecond(data.ReducedPerSecond);
        this.indexedPerSecond(data.IndexedPerSecond);
    }
}


export = indexingSpeed;
