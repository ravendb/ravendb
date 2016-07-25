import timeSeries = require("models/timeSeries/timeSeries");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import cssGenerator = require("common/cssGenerator");
import getKeysCommand = require("commands/timeSeries/getKeysCommand");

class timeSeriesType implements ICollectionBase {
    colorClass = "";
    private timeSeriesList: pagedList;
    private static typeColorMaps: resourceStyleMap[] = [];
    keysCount = ko.observable<number>(0);
    timeSeriesCountWithThousandsSeparator = ko.computed(() => this.keysCount().toLocaleString());
    
    constructor(public name: string, public fields: string[], keysCount: number, private ownerTimeSeries: timeSeries) {
        this.keysCount(keysCount);
        this.colorClass = timeSeriesType.getTypeCssClass(this.name, ownerTimeSeries);
        /* TODO:
        this["Name"] = name;
        this["Fields"] = fields.join(', ');
        this["Keys"] = keysCount; */
    }

    activate() {
        ko.postbox.publish("ActivateType", this);
    }

    getKeys() {
        if (!this.timeSeriesList) {
            this.timeSeriesList = this.createPagedList();
        }

        return this.timeSeriesList;
    }

    static fromDto(dto: timeSeriesTypeDto, ts: timeSeries): timeSeriesType {
        return new timeSeriesType(dto.Type, dto.Fields, dto.KeysCount, ts);
    }

    static getTypeCssClass(type: string, ts: timeSeries): string {
        return cssGenerator.getCssClass(type, timeSeriesType.typeColorMaps, ts);
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchKeys(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.name;
        return list;
    }

    private fetchKeys(skip: number, take: number): JQueryPromise<pagedResultSet<any>> {
        return new getKeysCommand(this.ownerTimeSeries, skip, take, this.name, this.keysCount()).execute();
    }

    getEntityName() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Name", "Fields", "Keys"];
    }

    getId() {
        return this.name;
    }

    getUrl() {
        return this.getId();
    }
}

export = timeSeriesType;
