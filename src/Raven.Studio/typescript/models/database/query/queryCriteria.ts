/// <reference path="../../../../typings/tsd.d.ts"/>

import genUtils = require("common/generalUtils");
import querySort = require("models/database/query/querySort");

class queryCriteria {
    selectedIndex = ko.observable<string>();
    useAndOperator = ko.observable<boolean>(false);
    showFields = ko.observable<boolean>(false);
    indexEntries = ko.observable<boolean>(false);
    queryText = ko.observable<string>("FROM @all_docs");
    transformer = ko.observable<string>();
    transformerParameters = ko.observableArray<transformerParamDto>();
    sorts = ko.observableArray<querySort>([]);

    hasSorts: KnockoutComputed<boolean>;
    allDocumentsIndexSelected: KnockoutComputed<boolean>;

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

        this.hasSorts = ko.pureComputed(() => this.sorts().filter(x => x.fieldName()).length > 0);
        this.allDocumentsIndexSelected = ko.pureComputed(() => this.selectedIndex() === "dynamic");
    }

    updateUsing(storedQuery: storedQueryDto) {
        this.selectedIndex(storedQuery.indexName);
        this.queryText(storedQuery.queryText);
        this.showFields(storedQuery.showFields);
        this.indexEntries(storedQuery.indexEntries);
        this.useAndOperator(storedQuery.useAndOperator);

        const transformerDto = storedQuery.transformerQuery;
        if (transformerDto) {
            this.transformer(transformerDto.transformerName);
            this.transformerParameters(transformerDto.queryParams);
        } else {
            this.transformer(null);
            this.transformerParameters([]);
        }

        this.sorts(storedQuery.sorts.map(x => querySort.fromQuerySortString(x)));
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
        const indexName = this.selectedIndex();
        const queryText = this.queryText();
        const showFields = this.showFields();
        const useAnd = this.useAndOperator();
        const sorts = this.sorts().filter(x => x.fieldName()).map(x => x.toQuerySortString());
        let transformerQuery: transformerQueryDto = undefined;
        if (this.transformer()) {
            transformerQuery = {
                transformerName: this.transformer(),
                queryParams: this.transformerParameters()
            }
        }

        return {
            indexEntries: indexEntries,
            indexName: indexName,
            queryText: queryText,
            showFields: showFields,
            sorts: sorts,
            transformerQuery: transformerQuery,
            useAndOperator: useAnd,
            hash: genUtils.hashCode(indexName + (queryText || "") +
                sorts.join(",") +
                this.getTransformerQueryUrlPart() +
                showFields +
                indexEntries +
                useAnd)
        } as storedQueryDto;
    }

    setSelectedIndex(indexName: string) {
        this.queryText("FROM @all_docs");
        this.selectedIndex(indexName);
        this.transformer(null);
        this.transformerParameters([]);
        this.sorts([]);
    }
}

export = queryCriteria; 
