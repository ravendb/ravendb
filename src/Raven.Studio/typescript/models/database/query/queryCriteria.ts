/// <reference path="../../../../typings/tsd.d.ts"/>

import genUtils = require("common/generalUtils");
import querySort = require("models/database/query/querySort");
import queryUtil = require("common/queryUtil");

class queryCriteria {
    showFields = ko.observable<boolean>(false);
    indexEntries = ko.observable<boolean>(false);
    queryText = ko.observable<string>("");
    
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
            aceValidation: true
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
    }

    updateUsing(storedQuery: storedQueryDto) {
        this.queryText(storedQuery.queryText);
        this.showFields(storedQuery.showFields);
        this.indexEntries(storedQuery.indexEntries);
    }

    toStorageDto(): storedQueryDto {
        const indexEntries = this.indexEntries();
        const queryText = this.queryText();
        const showFields = this.showFields();

        return {
            indexEntries: indexEntries,
            queryText: queryText,
            showFields: showFields,
            hash: genUtils.hashCode((queryText || "") +
                showFields +
                indexEntries)
        } as storedQueryDto;
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
}

export = queryCriteria; 
