/// <reference path="../../../../typings/tsd.d.ts"/>
import rawTimeSeriesPolicy = require("models/database/documents/rawTimeSeriesPolicy");
import timeSeriesPolicy = require("models/database/documents/timeSeriesPolicy");
import timeSeriesNamedValues = require("models/database/documents/timeSeriesNamedValues");

class timeSeriesConfigurationEntry {

    disabled = ko.observable<boolean>(false);
    collection = ko.observable<string>();
    
    rawPolicy = ko.observable<rawTimeSeriesPolicy>(rawTimeSeriesPolicy.empty());
    policies = ko.observableArray<timeSeriesPolicy>([]);
    
    namedValues = ko.observableArray<timeSeriesNamedValues>([]);

    hasPoliciesOrRetentionConfig: KnockoutComputed<boolean>;
    hasNamedValuesConfig: KnockoutComputed<boolean>;
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        collection: this.collection,
        namedValues: this.namedValues
    });

    constructor(collection: string) {
        this.collection(collection);

        this.hasPoliciesOrRetentionConfig = ko.pureComputed(() => this.policies().length > 0 || (this.rawPolicy() && this.rawPolicy().hasRetention()));
        this.hasNamedValuesConfig = ko.pureComputed(() => this.namedValues().length > 0);

        this.initValidation();
       
        _.bindAll(this, "addPolicy", "removePolicy", "addNamedValues", "removeNamedValues");
    }
    
    withRetention(dto: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesCollectionConfiguration) {
        this.disabled(dto.Disabled);

        this.rawPolicy(new rawTimeSeriesPolicy(dto.RawPolicy));
        this.policies(dto.Policies.map(x => new timeSeriesPolicy(x)));
 
        this.linkPolicies();
    }
    
    withNamedValues(dto: System.Collections.Generic.Dictionary<string, string[]>) {
        this.namedValues(Object.entries(dto).map(([tsName, configuration]) => {
            return new timeSeriesNamedValues(tsName, configuration);
        }))
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
        
        this.namedValues.extend({
            validation: [
                {
                    validator: () => this.hasPoliciesOrRetentionConfig() || this.hasNamedValuesConfig(),
                    message: "No rollup policy, retention time or named value are defined."
                }, {
                    validator: () => {
                        const nonEmptyNames = this.namedValues()
                            .map(x => x.timeSeriesName())
                            .filter(x => x);
                        
                        return nonEmptyNames.length === 0 || _.union(nonEmptyNames).length === nonEmptyNames.length;
                    },
                    message: "Time Series names must be unique"
                }
            ]
        });
    }

    copyFrom(incoming: timeSeriesConfigurationEntry): this {
        this.disabled(incoming.disabled());
        this.collection(incoming.collection());
        
        this.rawPolicy().copyFrom(incoming.rawPolicy());
        this.policies(incoming.policies().map(x => timeSeriesPolicy.empty().copyFrom(x)));
        
        this.linkPolicies();
        
        this.namedValues(incoming.namedValues().map(x => timeSeriesNamedValues.empty().copyFrom(x)));
        
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

    addNamedValues() {
        const newValues = timeSeriesNamedValues.empty();
        newValues.hasFocus(true);
        this.namedValues.push(newValues);
    }
    
    removeNamedValues(item: timeSeriesNamedValues) {
        this.namedValues.remove(item);
    }

    toPoliciesDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesCollectionConfiguration {
        return {
            Disabled: this.disabled(),
            Policies: this.policies().map(x => x.toDto()),
            RawPolicy: this.rawPolicy().toDto()
        };
    }

    toNamedValuesDto(): System.Collections.Generic.Dictionary<string, string[]> {
        const result = {} as System.Collections.Generic.Dictionary<string, string[]>;

        this.namedValues().forEach(entry => {
            result[entry.timeSeriesName()] = entry.namedValues().map(x => x.name());
        });
        
        return result;
    }

    static empty() {
        const emptyRetentionDto: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesCollectionConfiguration = {
            Disabled: false,
            RawPolicy: rawTimeSeriesPolicy.emptyPolicy,
            Policies: []
        };
        const entry = new timeSeriesConfigurationEntry("");
        entry.withRetention(emptyRetentionDto);
        entry.withNamedValues({});
        return entry;
    }
}

export = timeSeriesConfigurationEntry;
