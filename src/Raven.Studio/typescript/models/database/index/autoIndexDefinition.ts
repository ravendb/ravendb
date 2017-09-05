/// <reference path="../../../../typings/tsd.d.ts"/>

import indexDefinition = require("models/database/index/indexDefinition");

class autoIndexField {
    fieldName = ko.observable<string>();
    hasSearch = ko.observable<boolean>();
    hasExact = ko.observable<boolean>();
    operation = ko.observable<string>();

    // fieldStr has form: <Name:Count,Operation:Count>
    constructor(fieldStr: string, fields: Array<string>) {
        
        const parsedString = this.parse(fieldStr);
        const fieldName = parsedString['Name'];
        this.fieldName(fieldName);
        this.hasSearch(_.includes(fields, autoIndexField.searchFieldName(fieldName)));
        this.hasExact(_.includes(fields, autoIndexField.exactFieldName(fieldName)));
    }
    
    static searchFieldName(fieldName: string) {
        return "search(" + fieldName + ")";
    }
    
    static exactFieldName(fieldName: string) {
        return "exact(" + fieldName + ")";
    }

    protected parse(str: string): dictionary<string> {
        if (!str.startsWith("<") || !str.endsWith(">")) {
            throw new Error("Invalid field: " + str);
        }


        str = str.substring(1, str.length - 1);

        const result = {} as dictionary<string>;
        
        str.split(",").map(token => {
            const [key, value] = token.split(":");
            result[key] = value;
        });

        return result;
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

class autoIndexMapField extends autoIndexField {

    constructor(fieldStr: string, fields: Array<string>) {
        super(fieldStr, fields);

        const parsedString = this.parse(fieldStr);
        this.operation(parsedString['Operation']);
    }
}

class autoIndexDefinition extends indexDefinition {
    mapFields = ko.observableArray<autoIndexMapField>(); 
    reduceFields = ko.observableArray<autoIndexField>();
    collection = ko.observable<string>();

    constructor(dto: Raven.Client.Documents.Indexes.IndexDefinition) {
        super(dto);
        
        const fields = Object.keys(dto.Fields);
        this.parseMapFields(dto.Maps[0], fields);
        if (this.hasReduce()) {
            this.parseReduceFields(dto.Reduce, fields);
        }
    }

    private parseMapFields(mapStr: string, fieldNames: Array<string>) {
        const firstColonIdx = mapStr.indexOf(":");
        const collection = mapStr.substr(0, firstColonIdx);
        let fields = mapStr.substring(firstColonIdx + 1);

        this.collection(collection);
        // trim [ and ]
        fields = fields.substring(1, fields.length - 1);
        const fieldsList = fields.split(";");
        
        this.mapFields(fieldsList.map(x => new autoIndexMapField(x, fieldNames)));
    }

    private parseReduceFields(reduceStr: string, fieldNames: Array<string>) {
        const firstColonIdx = reduceStr.indexOf(":");
        let fields = reduceStr.substring(firstColonIdx + 1);
        // trim [ and ]
        fields = fields.substring(1, fields.length - 1);
        const fieldsList = fields.split(";");

        this.reduceFields(fieldsList.map(x => new autoIndexField(x, fieldNames)));
    }
}

export = autoIndexDefinition;
