import timeSeries = require("models/timeSeries/timeSeriesDocument");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import cssGenerator = require("common/cssGenerator");
import timeSeriesPointsCommand = require("commands/timeSeries/getTimeSeriesPointsCommand");

class timeSeriesKey implements ICollectionBase {
	colorClass = "";
	name = "";
    private timeSeriesList: pagedList;
    private static prefixColorMaps: resourceStyleMap[] = [];
    timeSeriesCount = ko.observable<number>(0);
    timeSeriesCountWithThousandsSeparator = ko.computed(() => this.timeSeriesCount().toLocaleString());
    
    constructor(public prefix: string, public key: string, private ownerTimeSeries: timeSeries, count: number = 0) {
        this.timeSeriesCount(count);
        this.name = prefix + "/" + key;
        this.colorClass = timeSeriesKey.getPrefixCssClass(this.prefix, ownerTimeSeries);
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
        return new timeSeriesKey(dto.Prefix, dto.Key, ts, dto.Count);
    }

    static getPrefixCssClass(prefix: string, ts: timeSeries): string {
        return cssGenerator.getCssClass(prefix, timeSeriesKey.prefixColorMaps, ts);
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchTimeSeries(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.name;
        return list;
    }

    private fetchTimeSeries(skip: number, take: number): JQueryPromise<pagedResultSet> {
        return new timeSeriesPointsCommand(this.ownerTimeSeries, skip, take, this.key, this.prefix).execute();
    }
} 

export = timeSeriesKey;