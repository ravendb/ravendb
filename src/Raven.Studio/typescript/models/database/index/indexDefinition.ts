/// <reference path="../../../../typings/tsd.d.ts"/>
import indexFieldOptions = require("models/database/index/indexFieldOptions");
import additionalSource = require("models/database/index/additionalSource");
import additionalAssembly = require("models/database/index/additionalAssemblyModel");
import configurationItem = require("models/database/index/configurationItem");
import validateNameCommand = require("commands/resources/validateNameCommand");
import generalUtils = require("common/generalUtils");
import compoundField from "models/database/index/compoundField";

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
    //isTestIndex = ko.observable<boolean>(false);
    
    fields = ko.observableArray<indexFieldOptions>();
    compoundFields = ko.observableArray<compoundField>([]);
    hasDuplicateFieldsNames: KnockoutComputed<boolean>;
    
    additionalSources = ko.observableArray<additionalSource>();
    additionalAssemblies = ko.observableArray<additionalAssembly>();
    
    defaultFieldOptions = ko.observable<indexFieldOptions>(null);
    isAutoIndex = ko.observable<boolean>(false);

    hasReduce = ko.observable<boolean>(false);
    outputReduceToCollection = ko.observable<boolean>();
    reduceOutputCollectionName = ko.observable<string>();
    
    archivedDataProcessingBehavior = ko.observable<Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior>(null);
    
    createReferencesToResultsCollection = ko.observable<boolean>();
    patternForReferencesToReduceOutputCollection = ko.observable<string>();
    collectionNameForReferenceDocuments = ko.observable<string>();

    numberOfFields = ko.pureComputed(() => this.fields().length);
    numberOfConfigurationFields = ko.pureComputed(() => this.configuration() ? this.configuration().length : 0);
    numberOfCompoundFields = ko.pureComputed(() => this.compoundFields().length);

    configuration = ko.observableArray<configurationItem>();
    lockMode: Raven.Client.Documents.Indexes.IndexLockMode;

    priority = ko.observable<Raven.Client.Documents.Indexes.IndexPriority>();
    deploymentMode = ko.observable<Raven.Client.Documents.Indexes.IndexDeploymentMode>();

    customAnalyzers = ko.observableArray<string>();

    searchEngine = ko.observable<Raven.Client.Documents.Indexes.SearchEngineType>();

    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Documents.Indexes.IndexDefinition) {
        this.isAutoIndex(dto.Type.startsWith("Auto"));

        this.name(dto.Name);
        this.maps(dto.Maps.map(x => new mapItem(x)));
        this.reduce(dto.Reduce);
        this.hasReduce(!!dto.Reduce);
        this.deploymentMode(dto.DeploymentMode);
        //this.isTestIndex(dto.IsTestIndex);
        
        this.outputReduceToCollection(!!dto.OutputReduceToCollection);
        this.reduceOutputCollectionName(dto.OutputReduceToCollection);
        
        this.archivedDataProcessingBehavior(dto.ArchivedDataProcessingBehavior);
        
        this.createReferencesToResultsCollection(!!dto.PatternForOutputReduceToCollectionReferences);
        this.patternForReferencesToReduceOutputCollection(dto.PatternForOutputReduceToCollectionReferences);
        this.collectionNameForReferenceDocuments(dto.PatternReferencesCollectionName);

        this.fields(Object.entries(dto.Fields).map(([indexName, fieldDto]) =>
            new indexFieldOptions(indexName, fieldDto, this.hasReduce, this.searchEngine, indexFieldOptions.defaultFieldOptions(this.hasReduce, this.searchEngine))));
        
        if (dto.CompoundFields) {
            this.compoundFields(dto.CompoundFields.map(compoundField.fromDto));
        } else {
            this.compoundFields([]);
        }
        
        const defaultFieldOptions = this.fields().find(x => x.name() === indexFieldOptions.DefaultFieldOptions);
        if (defaultFieldOptions) {
            this.defaultFieldOptions(defaultFieldOptions);
            
            defaultFieldOptions.parent(indexFieldOptions.globalDefaults(this.hasReduce, this.searchEngine));
            this.fields.remove(defaultFieldOptions);

            this.fields().forEach(field => {
                field.parent(defaultFieldOptions);
            });

            this.initDefaultFieldOptionsSubscriptions();
        }
        
        this.lockMode = dto.LockMode;
        this.priority(dto.Priority);
        this.configuration(this.parseConfiguration(dto.Configuration));

        if (dto.AdditionalSources) {
            this.additionalSources(Object.entries(dto.AdditionalSources).map(([name, code]) => additionalSource.create(name, code)));
        }
        
        if (dto.AdditionalAssemblies) {
            this.additionalAssemblies(dto.AdditionalAssemblies.map(assembly => new additionalAssembly(assembly)));
        }
        
        this.hasDuplicateFieldsNames = ko.pureComputed(() => {
            const nonEmptyFields = this.fields().filter(x => x.name());
            return new Set(nonEmptyFields.map(x => x.name())).size !== nonEmptyFields.length;
        });
        
        this.searchEngine.subscribe((engine: Raven.Client.Documents.Indexes.SearchEngineType) => {
            this.fields().forEach(x => x.searchEngine(engine));
        })

        if (!this.isAutoIndex()) {
            this.initValidation();
        }
    } 
    
    private initValidation() {
        
        const checkIndexName = (val: string,
                                params: any,
                                callback: (currentValue: string, errorMessageOrValidationResult: string | boolean) => void) => {
                                    new validateNameCommand('Index', val)
                                        .execute()
                                        .done((result) => {
                                            if (result.IsValid) {
                                                callback(this.name(), true);
                                            } else {
                                                callback(this.name(), result.ErrorMessage);
                                            }
                                        })
                               };
        
        this.name.extend({
            required: true,
            validation: [
                {
                    async: true,
                    validator: generalUtils.debounceAndFunnel(checkIndexName)
                }]
        });
        
        this.fields.extend({
           validation: [
               {
                   validator: () => !this.hasDuplicateFieldsNames()
               }
           ] 
        });

        this.reduce.extend({
            required: {
                onlyIf: () => this.hasReduce() && !this.reduce()
            },
            validation: [
                {
                    validator: (reduceContent: string) => (this.hasReduce() && reduceContent && reduceContent.trim()) ||
                                                           !this.hasReduce(),
                    message: `Reduce function is empty`
                }
            ]
        });

        this.reduceOutputCollectionName.extend({
            required: {
                onlyIf: () => this.hasReduce() && this.outputReduceToCollection()
            }
        });

        this.patternForReferencesToReduceOutputCollection.extend({
            required: {
                onlyIf: () => this.hasReduce() && this.createReferencesToResultsCollection()
            }
        });
        
        this.collectionNameForReferenceDocuments.extend({
            validation: [
                {
                    validator: (value: string) => !value || value !== this.reduceOutputCollectionName(),
                    message: 'Name for Referencing Collection cannot be the same as the Reduce Output Collection.'
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            name: this.name,
            reduce: this.reduce,
            reduceOutputCollectionName: this.reduceOutputCollectionName,
            patternForReferencesToReduceOutputCollection: this.patternForReferencesToReduceOutputCollection,
            collectionNameForReferenceDocuments: this.collectionNameForReferenceDocuments,
            fields: this.fields,
            compoundFields: this.compoundFields,
        });
    }
    
    private initDefaultFieldOptionsSubscriptions() {
        this.defaultFieldOptions().indexing.subscribe(() => {
            this.fields().forEach(x => {
                if (x.indexing() === null) {
                    x.indexing.valueHasMutated();
                }
            })
        });

        this.defaultFieldOptions().fullTextSearch.subscribe(() => {
            this.fields().forEach(x => {
                if (x.fullTextSearch() === null) {
                    x.fullTextSearch.valueHasMutated();
                }
            })
        })

        this.defaultFieldOptions().analyzer.subscribe(() => {
            if (this.defaultFieldOptions().indexing() === "Search") {
                this.fields().forEach(x => {
                    if (x.indexing() === null) {
                        x.computeAnalyzer();
                    }
                })
            }
        })
    }
    
    private parseConfiguration(config: Raven.Client.Documents.Indexes.IndexConfiguration): Array<configurationItem> {
        return config ? Object.entries(config).map(([key, value]) => new configurationItem(key, value)) : [];
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

        return result;
    }
    
    private additionalSourceToDto(): dictionary<string> {
        if (!this.additionalSources().length) {
            return null;
        }
        const result = {} as dictionary<string>;
        
        this.additionalSources().forEach(source => {
            result[source.name()] = source.code();
        });
        
        return result;
    }

    toDto(): Raven.Client.Documents.Indexes.IndexDefinition {
        return {
            Name: this.name(),
            Maps: this.maps().map(m => m.map()),
            Reduce: this.reduce(),
            Type: this.detectIndexType(),
            SourceType: "None",
            LockMode: this.lockMode,
            Priority: this.priority(),
            DeploymentMode: this.deploymentMode(),
            Configuration: this.configurationToDto(),
            Fields: this.fieldToDto(),
            ArchivedDataProcessingBehavior: this.archivedDataProcessingBehavior(),
            OutputReduceToCollection: this.hasReduce() &&
                                      this.reduce()    &&
                                      this.outputReduceToCollection() ? 
                                           this.reduceOutputCollectionName() : null,
            PatternForOutputReduceToCollectionReferences: this.hasReduce() &&
                                                          this.reduce()    &&
                                                          this.outputReduceToCollection() &&
                                                          this.createReferencesToResultsCollection() && 
                                                          this.patternForReferencesToReduceOutputCollection() ? 
                                                               this.patternForReferencesToReduceOutputCollection() : null,
            PatternReferencesCollectionName: this.hasReduce() &&
                                             this.reduce()    &&
                                             this.outputReduceToCollection() &&
                                             this.createReferencesToResultsCollection() &&
                                             this.collectionNameForReferenceDocuments() ?
                                                this.collectionNameForReferenceDocuments() : null,
            AdditionalSources: this.additionalSourceToDto(),
            AdditionalAssemblies: this.additionalAssemblies().map(assembly => assembly.toDto()),
            CompoundFields: 
                this.compoundFields().length ? this.compoundFields().map(f => [f.field1(), f.field2()]) : null
        }
    }

    addMap() {
        const map = new mapItem("");
        this.maps.push(map);
    }

    addField() {
        const field = indexFieldOptions.empty(this.hasReduce, this.searchEngine);
        
        field.addCustomAnalyzers(this.customAnalyzers());
        
        if (this.defaultFieldOptions()) {
            field.parent(this.defaultFieldOptions());
        }
        
        this.fields.unshift(field);
    }

    addDefaultField() {
        const fieldOptions = indexFieldOptions.defaultFieldOptions(this.hasReduce, this.searchEngine);
        fieldOptions.addCustomAnalyzers(this.customAnalyzers());
        this.defaultFieldOptions(fieldOptions);

        this.fields().forEach(field => {
            field.parent(fieldOptions);
        });
        
        this.initDefaultFieldOptionsSubscriptions();
    }

    addConfigurationOption() {
        this.configuration.unshift(configurationItem.empty());
    }

    removeConfigurationOption(item: configurationItem) {
        this.configuration.remove(item);
    }

    removeDefaultFieldOptions() {
        this.defaultFieldOptions(null);

        this.fields().forEach(field => {
            field.parent(indexFieldOptions.defaultFieldOptions(this.hasReduce, this.searchEngine));
        });
    }

    addAssembly() {
        const newAssembly = additionalAssembly.empty();
        this.additionalAssemblies.unshift(newAssembly);
    }
    
    removeAssembly(assemblyItem: additionalAssembly) {
       this.additionalAssemblies.remove(assemblyItem);
    }
    
    setMapsAndReduce(maps: string[], reduce: string) {
        this.maps(maps.map(x => new mapItem(x)));
        this.reduce(reduce);
    }

    registerCustomAnalyzers(analyzerNames: string[]) {
        this.customAnalyzers(analyzerNames);
        
        this.fields().forEach(x => x.addCustomAnalyzers(analyzerNames));

        const defaultFieldOptions = this.defaultFieldOptions();
        if (defaultFieldOptions) {
            defaultFieldOptions.addCustomAnalyzers(analyzerNames);
        }
    }
    
    static empty(): indexDefinition {
        return new indexDefinition({
            Fields: {},
            Maps: [""],
            Name: "",
            LockMode: "Unlock",
            Reduce: undefined,
            Priority: "Normal",
            DeploymentMode: null,
            Configuration: null,
            Type: "Map",
            SourceType: "None",
            OutputReduceToCollection: null,
            ArchivedDataProcessingBehavior: null,
            AdditionalSources: null,
            AdditionalAssemblies: null,
            PatternForOutputReduceToCollectionReferences: null,
            PatternReferencesCollectionName: null,
            CompoundFields: []
        });
    }
}

export = indexDefinition;
