/// <reference path="../../../../typings/tsd.d.ts"/>

import indexFieldOptions = require("models/database/index/indexFieldOptions");
import configuration = require("configuration");
import configurationItem = require("models/database/index/configurationItem");

class mapItem {
    map = ko.observable<string>();

    validationGroup: KnockoutObservable<any>;

    constructor(map: string) {
        this.map(map);

        this.initValidation();
    }

    private initValidation() {
        this.map.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            map: this.map
        });
    }
}

class indexDefinition {

    name = ko.observable<string>();
    maps = ko.observableArray<mapItem>();
    reduce = ko.observable<string>();
    isTestIndex = ko.observable<boolean>(false);
    isSideBySideIndex = ko.observable<boolean>(false);
    fields = ko.observableArray<indexFieldOptions>();
    defaultFieldOptions = ko.observable<indexFieldOptions>();

    numberOfFields = ko.pureComputed(() => this.fields().length);
    numberOfConfigurationFields = ko.pureComputed(() => this.configuration() ? this.configuration().length : 0);

    configuration = ko.observableArray<configurationItem>();
    maxIndexOutputsPerDocument = ko.observable<number>();
    lockMode: Raven.Abstractions.Indexing.IndexLockMode;
    indexStoragePath = ko.observable<string>();

    hasReduce = ko.observable<boolean>(false);

    validationGroup: KnockoutObservable<any>;

    constructor(dto: Raven.Client.Indexing.IndexDefinition) {
        this.name(dto.Name);
        this.maps(dto.Maps.map(x => new mapItem(x)));
        this.reduce(dto.Reduce);
        this.isTestIndex(dto.IsTestIndex);
        this.isSideBySideIndex(dto.IsSideBySideIndex);
        this.fields(_.map(dto.Fields, (fieldDto, indexName) => new indexFieldOptions(indexName, fieldDto, indexFieldOptions.defaultFieldOptions())));
        const defaultFieldOptions = this.fields().find(x => x.name() === indexFieldOptions.DefaultFieldOptions);
        if (defaultFieldOptions) {
            this.defaultFieldOptions(defaultFieldOptions);
            defaultFieldOptions.parent(indexFieldOptions.globalDefaults());
            this.fields.remove(defaultFieldOptions);

            this.fields().forEach(field => {
                field.parent(defaultFieldOptions);
            });
        }
        this.lockMode = dto.LockMode;
        this.configuration(this.parseConfiguration(dto.Configuration));

        const existingMaxIndexOutputs = this.configuration().find(x => x.key() === configuration.indexing.maxMapIndexOutputsPerDocument);
        if (existingMaxIndexOutputs) {
            this.maxIndexOutputsPerDocument(parseInt(existingMaxIndexOutputs.value()));
            this.configuration.remove(existingMaxIndexOutputs);
        }

        const existingIndexStoragePath = this.configuration().find(x => x.key() === configuration.indexing.indexStoragePath);
        if (existingIndexStoragePath && existingIndexStoragePath.value()) {
            this.indexStoragePath(existingIndexStoragePath.value());
            this.configuration.remove(existingIndexStoragePath);
        }

        this.initValidation();
    }

    private initValidation() {

        const rg1 = /^[^\\]*$/; // forbidden character - backslash
        this.name.extend({
            required: true,
            validation: [
                {
                    validator: (val: string) => rg1.test(val),
                    message: "Can't use backslash in index name"
                }]
        });

        this.reduce.extend({
            required: {
                onlyIf: () => this.hasReduce()
            }
        })

        this.validationGroup = ko.validatedObservable({
            name: this.name,
            reduce: this.reduce
        });
    }

    private parseConfiguration(config: Raven.Client.Indexing.IndexConfiguration): Array<configurationItem> {
        const configurations = [] as configurationItem[];

        if (config) {
            _.forIn(config, (value, key) => {
                configurations.push(new configurationItem(key, value));
            });
        }

        return configurations;
    }

    private detectIndexType(): Raven.Client.Data.Indexes.IndexType {
        return this.reduce() ? "MapReduce" : "Map";
    }

    private fieldToDto(): dictionary<Raven.Client.Indexing.IndexFieldOptions> {
        const fields = {} as dictionary<Raven.Client.Indexing.IndexFieldOptions>;

        this.fields().forEach((indexField: indexFieldOptions) => {
            fields[indexField.name()] = indexField.toDto();
        });

        if (this.defaultFieldOptions()) {
            fields[indexFieldOptions.DefaultFieldOptions] = this.defaultFieldOptions().toDto();
        }

        return fields;
    }

    private configurationToDto(): Raven.Client.Indexing.IndexConfiguration {
        const result = {} as Raven.Client.Indexing.IndexConfiguration;

        this.configuration().forEach((configItem: configurationItem) => {
            result[configItem.key()] = configItem.value();
        });

        if (_.isNumber(this.maxIndexOutputsPerDocument())) {
            result[configuration.indexing.maxMapIndexOutputsPerDocument] = this.maxIndexOutputsPerDocument().toString();
        }

        if (this.indexStoragePath()) {
            result[configuration.indexing.indexStoragePath] = this.indexStoragePath();
        }

        return result;
    }

    toDto(): Raven.Client.Indexing.IndexDefinition {
        return {
            Name: this.name(),
            Maps: this.maps().map(m => m.map()),
            Reduce: this.reduce(),
            IndexId: null,
            Type: this.detectIndexType(),
            LockMode: this.lockMode,
            Configuration: this.configurationToDto(),
            Fields: this.fieldToDto(),
            IsSideBySideIndex: false, //TODO side by side
            IsTestIndex: false, //TODO: test indexes
        }
    }

    addMap() {
        const map = new mapItem("");
        this.maps.push(map);
    }

    addField() {
        const field = indexFieldOptions.empty();
        if (this.defaultFieldOptions()) {
            field.parent(this.defaultFieldOptions());
        }
        this.fields.push(field);
    }

    addDefaultField() {
        const fieldOptions = indexFieldOptions.defaultFieldOptions();
        this.defaultFieldOptions(fieldOptions);

        this.fields().forEach(field => {
            field.parent(fieldOptions);
        });
    }

    addConfigurationOption() {
        this.configuration.push(configurationItem.empty());
    }

    removeConfigurationOption(item: configurationItem) {
        this.configuration.remove(item);
    }

    removeDefaultFieldOptions() {
        this.defaultFieldOptions(null);

        this.fields().forEach(field => {
            field.parent(indexFieldOptions.defaultFieldOptions());
        });
    }

    static empty(): indexDefinition {
        return new indexDefinition({
            Fields: {},
            IndexId: null,
            Maps: [""],
            Name: "",
            LockMode: "Unlock",
            Reduce: undefined,
            Configuration: null,
            IsSideBySideIndex: false,
            IsTestIndex: false,
            Type: "Map"
        });
    }
}

export = indexDefinition;
