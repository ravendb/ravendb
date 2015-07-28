class timeSeriesKey implements documentBase {
    Type: string;
    Key: string;
    PointsCount: number;

    constructor(dto: timeSeriesKeyDto) {
        this.Type = dto.Type;
        this.Key = dto.Key;
        this.PointsCount = dto.PointsCount;
    }

    getEntityName() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Key", "Points Count"];
    }

    getId() {
        return this.Type + "/" + this.Key;
    }

    getUrl() {
        return this.getId();
    }
} 

export = timeSeriesKey;