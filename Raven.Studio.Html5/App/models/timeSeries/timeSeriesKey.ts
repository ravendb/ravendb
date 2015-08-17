import pagedList = require("common/pagedList");
import timeSeries = require("models/timeSeries/timeSeries");

class timeSeriesKey implements documentBase {
    Type: string;
    Fields: string[];
    Key: string;
    Points: number;

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
} 

export = timeSeriesKey;