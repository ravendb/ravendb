class timeSeriesPoint implements documentBase {
    Prefix: string;
    Key: string;
    At: Moment; 
    Values: number[]; 

    constructor(dto: timeSeriesPointDto) {
        this.Prefix = dto.Prefix;
        this.Key = dto.Key;
        this.At = moment(dto.At);
        this.Values = dto.Values;
    }

    getEntityName() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        var columns = ["At", "Values"];
        return columns;
    }

    getId() {
        return this.Prefix + "/" + this.Key;
    }

    getUrl() {
        return this.getId();
    }
} 

export = timeSeriesPoint;