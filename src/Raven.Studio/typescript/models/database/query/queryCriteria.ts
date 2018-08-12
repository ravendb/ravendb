/// <reference path="../../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");
import queryUtil = require("common/queryUtil");

class queryCriteria {

    name = ko.observable<string>("");
    showFields = ko.observable<boolean>(false);
    indexEntries = ko.observable<boolean>(false);
    queryText = ko.observable<string>("");
    queryParameters = ko.observableArray<[string, string]>();
    metadataOnly = ko.observable<boolean>(false);
    recentQuery = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup;

    static empty() {
        const criteria = new queryCriteria();
        return criteria;
    }

    constructor() {
        this.initObservables();
        this.initValidation();
    }
    
    private initValidation() {
        this.queryText.extend({
            // We want to be able to send invalid queries in order to get the server side error as well.
            // aceValidation: true
        });
        
        this.validationGroup = ko.validatedObservable({
            queryText: this.queryText
        })
    }

    private initObservables() {
        this.showFields.subscribe(showFields => {
            if (showFields && this.indexEntries()) {
                this.indexEntries(false);
            }
        });

        this.indexEntries.subscribe(indexEntries => {
            if (indexEntries && this.showFields()) {
                this.showFields(false);
            }
        });

        this.queryText.subscribe(queryText => {
            this.mergeParameters(queryText);
        });
    }

    updateUsing(storedQuery: storedQueryDto) {
        this.queryText(storedQuery.queryText);
        this.recentQuery(storedQuery.recentQuery);
    }

    toStorageDto(): storedQueryDto {
        const name = this.name();
        const queryText = this.queryText();

        return {
            name: name,
            queryText: queryText,
            recentQuery: this.recentQuery(),
            modificationDate: moment().format("YYYY-MM-DD HH:mm"),
            hash: genUtils.hashCode(name + (queryText || "")) } as storedQueryDto;
    }

    setSelectedIndex(indexName: string) {
        let rql = "from ";
        if (indexName.startsWith(queryUtil.DynamicPrefix)) {
            rql += indexName.substring(queryUtil.DynamicPrefix.length);
        } else if (indexName === queryUtil.AllDocs) {
            rql += "@all_docs";
        } else {
            rql += "index '" + indexName + "'";
        }
        this.queryText(rql);
    }

    copyFrom(incoming: queryDto) {
        this.name("");
        this.queryText(incoming.queryText);
        this.recentQuery(incoming.recentQuery);
    }

    private mergeParameters(queryText: string) {
        const regex = /[$]\w+/g;
        let match;
        const keys = new Set<string>();
        while (match = regex.exec(queryText))
        {
            keys.add(match[0]);
        }
        this.queryParameters.remove(([key, value]) => {
            if (value){
                keys.delete(key);
            } 
            return !value;
        });
        keys.forEach(key => this.queryParameters.push([key,  ""]));
    }
}

export = queryCriteria; 
