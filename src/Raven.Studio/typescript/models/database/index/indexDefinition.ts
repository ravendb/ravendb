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
    fields = ko.observableArray<indexFieldOptions>();
    defaultFieldOptions = ko.observable<indexFieldOptions>();
    isAutoIndex = ko.observable<boolean>(false);

    outputReduceToCollection = ko.observable<string>();

    numberOfFields = ko.pureComputed(() => this.fields().length);
    numberOfConfigurationFields = ko.pureComputed(() => this.configuration() ? this.configuration().length : 0);

    configuration = ko.observableArray<configurationItem>();
    lockMode: Raven.Client.Documents.Indexes.IndexLockMode;
    indexStoragePath = ko.observable<string>();

    priority = ko.observable<Raven.Client.Documents.Indexes.IndexPriority>();

    hasReduce = ko.observable<boolean>(false);

    validationGroup: KnockoutValidationGroup;
    renameValidationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Documents.Indexes.IndexDefinition) {
        this.isAutoIndex(dto.Type.startsWith("Auto"));

        this.name(dto.Name);
        this.maps(dto.Maps.map(x => new mapItem(x)));
        this.reduce(dto.Reduce);
        this.hasReduce(!!dto.Reduce);
        this.isTestIndex(dto.IsTestIndex);
        this.outputReduceToCollection(dto.OutputReduceToCollection);
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
        this.priority(dto.Priority);
        this.configuration(this.parseConfiguration(dto.Configuration));

        const existingIndexStoragePath = this.configuration().find(x => x.key() === configuration.indexing.storagePath);
        if (existingIndexStoragePath && existingIndexStoragePath.value()) {
            this.indexStoragePath(existingIndexStoragePath.value());
            this.configuration.remove(existingIndexStoragePath);
        }

        if (!this.isAutoIndex()) {
            this.initValidation();
        } 
    }

    private initValidation() {

        const rg1 = /^[^\\]*$/; // forbidden character - backslash
        this.name.extend({
            required: true,
            validation: [
                {
                    validator: (val: string) => rg1.test(val),
                    message: "Can't use backslash in index name."
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

        this.renameValidationGroup = ko.validatedObservable({
            name: this.name
        });
    }

    private parseConfiguration(config: Raven.Client.Documents.Indexes.IndexConfiguration): Array<configurationItem> {
        const configurations = [] as configurationItem[];

        if (config) {
            _.forIn(config, (value, key) => {
                configurations.push(new configurationItem(key, value));
            });
        }

        return configurations;
    }

    private detectIndexType(): Raven.Client.Documents.Indexes.IndexType {
        return this.reduce() ? "MapReduce" : "Map";
    }

    private fieldToDto(): dictionary<Raven.Client.Documents.Indexes.IndexFieldOptions> {
        const fields = {} as dictionary<Raven.Client.Documents.Indexes.IndexFieldOptions>;

        this.fields().forEach((indexField: indexFieldOptions) => {
            fields[indexField.name()] = indexField.toDto();
        });

        if (this.defaultFieldOptions()) {
            fields[indexFieldOptions.DefaultFieldOptions] = this.defaultFieldOptions().toDto();
        }

        return fields;
    }

    private configurationToDto(): Raven.Client.Documents.Indexes.IndexConfiguration {
        const result = {} as Raven.Client.Documents.Indexes.IndexConfiguration;

        this.configuration().forEach((configItem: configurationItem) => {
            result[configItem.key()] = configItem.value();
        });

        if (this.indexStoragePath()) {
            result[configuration.indexing.storagePath] = this.indexStoragePath();
        }

        return result;
    }

    toDto(): Raven.Client.Documents.Indexes.IndexDefinition {
        return {
            Name: this.name(),
            Maps: this.maps().map(m => m.map()),
            Reduce: this.reduce(),
            IndexId: null,
            Type: this.detectIndexType(),
            LockMode: this.lockMode,
            Priority: this.priority(),
            Configuration: this.configurationToDto(),
            Fields: this.fieldToDto(),
            IsTestIndex: false, //TODO: test indexes
            OutputReduceToCollection: this.outputReduceToCollection()
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
            Priority: "Normal",
            Configuration: null,
            IsTestIndex: false,
            Type: "Map",
            OutputReduceToCollection: null
        });
    }
}

export = indexDefinition;
