class timeSeriesKey implements documentBase {
    Type: string;
    Key: string;
    Points: number;

    constructor(dto: timeSeriesKeyDto) {
        this.Type = dto.Type;
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