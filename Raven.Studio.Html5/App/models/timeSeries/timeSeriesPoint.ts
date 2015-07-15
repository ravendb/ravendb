class timeSeriesPoint implements documentBase {
    Type: string;
    Key: string;
    At: string; 
    Values: number[]; 

    constructor(type: string, key: string, dto: pointDto) {
        this.Type = type;
        this.Key = key;
        this.At = dto.At;
        this.Values = dto.Values;

        if (this.Values.length === 1) {
            this["Value"] = this.Values[0];
        } else {
            for (var i = 0; i < this.Values.length; i++) {
                this["Value " + (i + 1)] = this.Values[i];
            }
        }
        
    }

    getEntityName() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        var columns = ["At"];
        if (this.Values.length === 1) {
            columns.push("Value");
        } else {
            for (var i = 0; i < this.Values.length; i++) {
                columns.push("Value " + (i + 1));
            }
        }
        return columns;
    }

    getId() {
        return this.Type + "/" + this.Key;
    }

    getUrl() {
        return this.getId();
    }
} 

export = timeSeriesPoint;