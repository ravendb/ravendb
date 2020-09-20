/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import configuration = require("configuration");

class configurationItem {

    static readonly ServerWideIndexingConfigurationOptions = [        
        configuration.indexing.globalScratchSpaceLimit,
        configuration.indexing.cleanupInterval,
        configuration.indexing.maxTimeToWaitAfterFlushAndSyncWhenExceedingScratchSpaceLimit
    ];

    static readonly PerDatabaseIndexingConfigurationOptions: Array<string> = _.sortBy(_.pullAll(_.values<string>(configuration.indexing), 
                                                                                                 configurationItem.ServerWideIndexingConfigurationOptions));       
    
    key = ko.observable<string>();
    value = ko.observable<string>();

    validationGroup: KnockoutObservable<any>;
    dirtyFlag: () => DirtyFlag;

    constructor(key: string, value: string) {
        this.key(key);
        this.value(value);

        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.key, 
            this.value,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initValidation() {
        this.key.extend({
            required: true
        });

        this.value.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            key: this.key,
            value: this.value
        });
    }

    static empty() {
        return new configurationItem("", "");
    }
}

export = configurationItem;