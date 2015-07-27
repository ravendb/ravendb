import timeSeries = require("models/timeSeries/timeSeriesDocument");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import cssGenerator = require("common/cssGenerator");
import getPointsCommand = require("commands/timeSeries/getPointsCommand");

class timeSeriesKey implements ICollectionBase {
	colorClass = "";
	name = "";
    private timeSeriesList: pagedList;
    private static typeColorMaps: resourceStyleMap[] = [];
    pointsCount = ko.observable<number>(0);
    timeSeriesCountWithThousandsSeparator = ko.computed(() => this.pointsCount().toLocaleString());
    
    constructor(public type: string, public key: string, public valueLength: number, private ownerTimeSeries: timeSeries) {
        this.name = type + "/" + key;
        this.colorClass = timeSeriesKey.getTypeCssClass(this.type, ownerTimeSeries);
    }

    activate() {
		ko.postbox.publish("ActivateKey", this);
    }

    getTimeSeries() {
        if (!this.timeSeriesList) {
            this.timeSeriesList = this.createPagedList();
        }

        return this.timeSeriesList;
    }

	invalidateCache() {
		var timeSeriesList = this.getTimeSeries();
		timeSeriesList.invalidateCache();
	}

    static fromDto(dto: timeSeriesKeyDto, ts: timeSeries): timeSeriesKey {
        var _new = new timeSeriesKey(dto.Type, dto.Key, dto.NumberOfValues, ts);
        _new.pointsCount(dto.PointsCount);
        return _new;
    }

    static getTypeCssClass(type: string, ts: timeSeries): string {
        return cssGenerator.getCssClass(type, timeSeriesKey.typeColorMaps, ts);
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchTimeSeries(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.name;
        return list;
    }

    private fetchTimeSeries(skip: number, take: number): JQueryPromise<pagedResultSet> {
        return new getPointsCommand(this.ownerTimeSeries, skip, take, this.type, this.key, this.pointsCount()).execute();
    }
} 

export = timeSeriesKey;