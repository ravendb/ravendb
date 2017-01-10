/// <reference path="../../../../typings/tsd.d.ts"/>

import indexFieldOptions = require("models/database/index/indexFieldOptions");
import configuration = require("configuration");

class configurationItem {
    key = ko.observable<string>();
    value = ko.observable<string>();

    constructor(key: string, value: string) {
        this.key(key);
        this.value(value);
    }

    static empty() {
        return new configurationItem("", "");
    }
}

class indexDefinition {

    name = ko.observable<string>();
    maps = ko.observableArray<KnockoutObservable<string>>();
    reduce = ko.observable<string>();
    isTestIndex = ko.observable<boolean>(false);
    isSideBySideIndex = ko.observable<boolean>(false);
    fields = ko.observableArray<indexFieldOptions>();
    defaultFieldOptions = ko.observable<indexFieldOptions>();

    numberOfLuceneFields = ko.pureComputed(() => this.fields().length);
    numberOfConfigurationFields = ko.pureComputed(() => this.configuration() ? this.configuration().length : 0);

    configuration = ko.observableArray<configurationItem>();
    maxIndexOutputsPerDocument = ko.observable<number>();
    lockMode: Raven.Abstractions.Indexing.IndexLockMode;

    constructor(dto: Raven.Client.Indexing.IndexDefinition) {
        this.name(dto.Name);
        this.maps(dto.Maps.map(x => ko.observable<string>(x)));
        this.reduce(dto.Reduce);
        this.isTestIndex(dto.IsTestIndex);
        this.isSideBySideIndex(dto.IsSideBySideIndex);
        this.fields(_.map(dto.Fields, (fieldDto, indexName) => new indexFieldOptions(indexName, fieldDto)));
        const defaultFieldOptions = this.fields().find(x => x.name() === indexFieldOptions.DefaultFieldOptions);
        if (defaultFieldOptions) {
            this.defaultFieldOptions(defaultFieldOptions);
            this.fields.remove(defaultFieldOptions);
        }
        this.lockMode = dto.LockMode;
        this.configuration(this.parseConfiguration(dto.Configuration));

        const existingMaxIndexOutputs = this.configuration().find(x => x.key() === configuration.indexing.maxMapIndexOutputsPerDocument);
        if (existingMaxIndexOutputs) {
            this.maxIndexOutputsPerDocument(parseInt(existingMaxIndexOutputs.value()));
            this.configuration.remove(existingMaxIndexOutputs);
        }
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

        return result;
    }

    toDto(): Raven.Client.Indexing.IndexDefinition {
        return {
            Name: this.name(),
            Maps: this.maps().map(m => m()),
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

    addField() {
        this.fields.push(indexFieldOptions.empty());
    }

    addDefaultField() {
        const fieldOptions = indexFieldOptions.empty();
        fieldOptions.name(indexFieldOptions.DefaultFieldOptions);
        this.defaultFieldOptions(fieldOptions);
    }

    addConfigurationOption() {
        this.configuration.push(configurationItem.empty());
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
