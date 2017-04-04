/// <reference path="../../../../typings/tsd.d.ts"/>

import indexDefinition = require("models/database/index/indexDefinition");

class autoIndexField {
    fieldName = ko.observable<string>();
    sort = ko.observable<string>();
    operation = ko.observable<string>();

    constructor(fieldStr: string) {
        this.fieldName(this.getFieldData(fieldStr, "Name:", ","));
        this.sort(this.getFieldData(fieldStr,"Sort:", ","));
    }

    protected getFieldData(fieldStr: string, searchStr: string, endChar: string): string {
        let value = "";

        const location = fieldStr.indexOf(searchStr);
        if (location !== -1) {
            const leftStr = fieldStr.substr(location + searchStr.length);
            value = leftStr.substr(0, leftStr.indexOf(endChar));
        }

        return value;
    }
}

class autoIndexMapField extends autoIndexField{

    constructor(fieldStr: string) {
        super(fieldStr);
        this.operation(this.getFieldData(fieldStr, "Operation:", ">"));
    }
}

class autoIndexDefinition extends indexDefinition {
    mapFields = ko.observableArray<autoIndexMapField>(); 
    reduceFields = ko.observableArray<autoIndexField>(); 

    constructor(dto: Raven.Client.Documents.Indexes.IndexDefinition) {
        super(dto);

        this.parseMapFields(dto.Maps[0]);
        if (this.hasReduce()) {
            this.parseReduceFields(dto.Reduce);
        }
    }

    private parseMapFields(mapStr: string) {
        mapStr = mapStr.substr(mapStr.indexOf("["));
        const fieldsList = mapStr.split(";");

        this.mapFields(fieldsList.map(x => new autoIndexMapField(x)));
    }

    private parseReduceFields(reduceStr: string) {
        reduceStr = reduceStr.substr(reduceStr.indexOf("["));
        const fieldsList = reduceStr.split(";");

        this.reduceFields(fieldsList.map(x => new autoIndexField(x)));
    }
}

export = autoIndexDefinition;
