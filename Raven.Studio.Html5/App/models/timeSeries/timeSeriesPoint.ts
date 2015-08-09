class timeSeriesPoint implements documentBase {
    type: string;
    fields: string[];
    key: string;
    At: string; 

    constructor(type: string, fields: string[], key: string, dto: pointDto) {
        this.type = type;
        this.fields = fields;
        this.key = key;
        this.At = dto.At;

        for (var i = 0; i < this.fields.length; i++) {
            this[fields[i]] = dto.Values[i];
        }
    }

    getEntityName() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        var columns = ["At"];
        for (var i = 0; i < this.fields.length; i++) {
            columns.push(this.fields[i]);
        }
        return columns;
    }

    getId() {
        return this.type + "/" + this.key + "/" + this.At;
    }

    getUrl() {
        return this.getId();
    }
} 

export = timeSeriesPoint;