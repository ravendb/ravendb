import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import getPointsCommand = require("commands/timeSeries/getPointsCommand");
import timeSeries = require("models/timeSeries/timeSeries");

class timeSeriesKey implements documentBase {
    Type: string;
    Fields: string[];
    Key: string;
    Points: number;
    private pointsList: pagedList;

    constructor(dto: timeSeriesKeyDto, private ownerTimeSeries: timeSeries) {
        this.Type = dto.Type.Type;
        this.Fields = dto.Type.Fields;
        this.Key = dto.Key;
        this.Points = dto.PointsCount;
    }

    getEntityName() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Key", "Points"];
    }

    getId() {
        return this.Type + "/" + this.Key;
    }

    getUrl() {
        return this.getId();
    }

    getPoints() {
        if (!this.pointsList) {
            this.pointsList = this.createPointsPagedList();
        }
        return this.pointsList;
    }

    private createPointsPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchPoints(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.Key;
        return list;
    }

    private fetchPoints(skip: number, take: number): JQueryPromise<pagedResultSet> {
        return new getPointsCommand(this.ownerTimeSeries, skip, take, this.Type, this.Fields, this.Key, this.Points).execute();
    }
} 

export = timeSeriesKey;