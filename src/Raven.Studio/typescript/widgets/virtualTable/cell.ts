/// <reference path="../../../typings/tsd.d.ts"/>

class cell {
    data = ko.observable<any>();
    resetFlag = false;

    static defaultTemplate = "defaultTemplate";
    static idTemplate = "idTemplate";
    static checkboxTemplate = "checkboxTemplate";
    static externalIdTemplate = "externalIdTemplate";
    static customTemplate = "customTemplate";
    static counterNameTemplate = "counterNameTemplate";
    static counterGroupTemplate = "counterGroupTemplate";
    static timeSeriesKeyTemplate = "timeSeriesKeyTemplate";

    constructor(data: any, public templateName: string) {
        this.update(data);
    }

    update(data: any) {
        if (this.isNumber(data)) {
            data = data.toLocaleString('en-US');
        }
        this.data(data);
    }

    private isNumber(o: any) {
        return typeof o === "number" && isFinite(o);
    }

    reset() {
        this.data("");       
        this.resetFlag = true; 
    }
}

export = cell;
