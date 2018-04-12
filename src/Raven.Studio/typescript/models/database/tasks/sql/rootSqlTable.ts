/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import innerSqlTable = require("models/database/tasks/sql/innerSqlTable");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class valueHolder<T> {
    value = ko.observable<T>();
    
    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        this.value.extend({
            required: true
        });
        
        this.validationGroup = ko.validatedObservable({
            value: this.value
        })
    }
}

class rootSqlTable extends abstractSqlTable {
    collectionName = ko.observable<string>();
    checked = ko.observable<boolean>(true);
    documentIdTemplate: KnockoutComputed<string>;
    
    customizeQuery = ko.observable<boolean>(false);
    query = ko.observable<string>();
    transformResults = ko.observable<boolean>(false);
    patchScript = ko.observable<string>();
    
    hasDuplicateProperties: KnockoutComputed<boolean>;
    
    testMode = ko.observable<Raven.Server.SqlMigration.Model.MigrationTestMode>("First");
    testPrimaryKeys = [] as Array<valueHolder<string>>;
    
    constructor() {
        super();
        this.documentIdTemplate = ko.pureComputed(() => {
            const templetePart = this.primaryKeyColumns().map(x => '{' + x.sqlName + '}').join("/");
            return this.collectionName() + "/" + templetePart;
        });
        
        this.hasDuplicateProperties = ko.pureComputed(() => {
            let hasDuplicates = false;
            
            if (this.checkForDuplicateProperties()) {
                hasDuplicates = true;
            }
            
            const refCheck = (reference: sqlReference) => {
                if (reference.action() === "embed" && reference.effectiveInnerTable()) {
                    if (reference.effectiveInnerTable().checkForDuplicateProperties()) {
                        hasDuplicates = true;    
                    }
                                         
                    reference.effectiveInnerTable().references().forEach(innerRef => refCheck(innerRef));    
                }
            };
            
            this.references().forEach(ref => refCheck(ref));
            
            return hasDuplicates;
        });
        this.primaryKeyColumns.subscribe(() => {
            this.testPrimaryKeys = this.primaryKeyColumns().map(() => new valueHolder());
        });
    }
    
    toDto(binaryToAttachment: boolean) {
        return {
            SourceTableName: this.tableName,
            SourceTableSchema: this.tableSchema,
            Name: this.collectionName(),
            Patch: this.transformResults() ? this.patchScript() : undefined,
            SourceTableQuery: this.customizeQuery() ? this.query() : undefined,
            NestedCollections: this.getEmbeddedReferencesDto(binaryToAttachment),
            LinkedCollections: this.getLinkedReferencesDto(),
            ColumnsMapping: this.getColumnsMapping(binaryToAttachment),
            AttachmentNameMapping: this.getAttachmentsMapping(binaryToAttachment)
        } as Raven.Server.SqlMigration.Model.RootCollection;
    }
    
    cloneForEmbed(parentReference: sqlReference): innerSqlTable {
        const table = new innerSqlTable(parentReference);
        
        table.tableName = this.tableName;
        table.tableSchema = this.tableSchema;
        table.documentColumns(this.documentColumns().map(x => x.clone()));
        table.primaryKeyColumns(this.primaryKeyColumns().map(x => x.clone()));
        table.references(this.references().map(x => {
            const clonedObject = x.clone();
            clonedObject.sourceTable = table;
            return clonedObject;
        }));
        
        return table;
    }
}


export = rootSqlTable;
 
