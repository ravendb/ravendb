/// <reference path="../../../../typings/tsd.d.ts"/>
import rawTimeSeriesPolicy = require("models/database/documents/rawTimeSeriesPolicy");
import timeSeriesPolicy = require("models/database/documents/timeSeriesPolicy");

class timeSeriesConfigurationEntry {

    disabled = ko.observable<boolean>();
    collection = ko.observable<string>();
    
    rawPolicy = ko.observable<rawTimeSeriesPolicy>();
    policies = ko.observableArray<timeSeriesPolicy>([]);
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        collection: this.collection
    });

    constructor(collection: string, dto: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesCollectionConfiguration) {
        this.collection(collection);
        this.disabled(dto.Disabled);
        
        this.rawPolicy(new rawTimeSeriesPolicy(dto.RawPolicy));
        this.policies(dto.Policies.map(x => new timeSeriesPolicy(x)));

        this.linkPolicies();
        
        this.initValidation();
        
        _.bindAll(this, "addPolicy", "removePolicy");
    }
    
    linkPolicies() {
        let previous = this.rawPolicy();
        for (let i = 0; i < this.policies().length; i++) {
            const item = this.policies()[i];
            item.previous(previous);
            previous = item;
        }
    }

    private initValidation() {
        this.collection.extend({
            required: true
        });
    }

    copyFrom(incoming: timeSeriesConfigurationEntry): this {
        this.disabled(incoming.disabled());
        this.collection(incoming.collection());
        
        this.rawPolicy().copyFrom(incoming.rawPolicy());
        this.policies(incoming.policies().map(x => timeSeriesPolicy.empty().copyFrom(x)));
        
        this.linkPolicies();
        
        return this;
    }
    
    addPolicy() {
        const newPolicy = timeSeriesPolicy.empty();
        this.policies.push(newPolicy);
        
        this.linkPolicies();
        newPolicy.name.isModified(false);
    }
    
    removePolicy(policy: timeSeriesPolicy) {
        this.policies.remove(policy);
        this.linkPolicies();
    }

    toDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesCollectionConfiguration {
        return {
            Disabled: this.disabled(),
            Policies: this.policies().map(x => x.toDto()),
            RawPolicy: this.rawPolicy().toDto()
        };
    }

    static empty() {
        return new timeSeriesConfigurationEntry("",
        {
            Disabled: false,
            RawPolicy: {
                RetentionTime: null,
                AggregationTime: null,
                Name: null
            },
            Policies: [] 
        });
    }
}

export = timeSeriesConfigurationEntry;
