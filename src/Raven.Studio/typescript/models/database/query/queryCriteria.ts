/// <reference path="../../../../typings/tsd.d.ts"/>

import genUtils = require("common/generalUtils");
import querySort = require("models/database/query/querySort");
import queryUtil = require("common/queryUtil");

class queryCriteria {
    showFields = ko.observable<boolean>(false);
    indexEntries = ko.observable<boolean>(false);
    queryText = ko.observable<string>("from @all_docs");
    transformer = ko.observable<string>();
    transformerParameters = ko.observableArray<transformerParamDto>();

    static empty() {
        const criteria = new queryCriteria();
        return criteria;
    }

    constructor() {
        this.initObservables();
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

        const transformerDto = storedQuery.transformerQuery;
        if (transformerDto) {
            this.transformer(transformerDto.transformerName);
            this.transformerParameters(transformerDto.queryParams);
        } else {
            this.transformer(null);
            this.transformerParameters([]);
        }
    }

    getTransformerQueryUrlPart() {
        if (this.transformer()) {
            const paramsUrl = this.transformerParameters()
                .map((param: transformerParamDto) => "tp-" + param.name + "=" + param.value)
                .join("&");

            return "&transformer=" + this.transformer() + (paramsUrl.length > 0 ? "&" + paramsUrl : "");
        } else {
            return "";
        }
    }

    toStorageDto(): storedQueryDto {
        const indexEntries = this.indexEntries();
        const queryText = this.queryText();
        const showFields = this.showFields();
        let transformerQuery: transformerQueryDto = undefined;
        if (this.transformer()) {
            transformerQuery = {
                transformerName: this.transformer(),
                queryParams: this.transformerParameters()
            }
        }

        return {
            indexEntries: indexEntries,
            queryText: queryText,
            showFields: showFields,
            transformerQuery: transformerQuery,
            hash: genUtils.hashCode((queryText || "") +
                this.getTransformerQueryUrlPart() +
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
        this.transformer(null);
        this.transformerParameters([]);
    }
}

export = queryCriteria; 
