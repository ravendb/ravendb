/// <reference path="../../../typings/tsd.d.ts"/>

class timeSeriesPoint implements documentBase {
    type: string;
    fields: string[];
    key: string;
    At: string; 
    values: number[]; 

    constructor(type: string, fields: string[], key: string, at: string, values: number[]) {
        this.type = type;
        this.fields = fields;
        this.key = key;
        this.At = at;
        this.values = values;

        for (var i = 0; i < this.fields.length; i++) {
            (<any>this)[fields[i]] = values[i];
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
