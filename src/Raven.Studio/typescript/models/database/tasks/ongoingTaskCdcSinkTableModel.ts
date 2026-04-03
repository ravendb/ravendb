/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class cdcSinkColumnMapping {
    sqlColumnName: string;
    sqlColumnType: string;
    propertyName: KnockoutObservable<string>;
    isPrimaryKey: boolean;

    constructor(sqlColumnName: string, sqlColumnType: string, propertyName: string, isPrimaryKey: boolean) {
        this.sqlColumnName = sqlColumnName;
        this.sqlColumnType = sqlColumnType;
        this.propertyName = ko.observable<string>(propertyName);
        this.isPrimaryKey = isPrimaryKey;
    }
}

class ongoingTaskCdcSinkTableModel {

    name = ko.observable<string>();
    sourceTableSchema = ko.observable<string>();
    sourceTableName = ko.observable<string>();
    patch = ko.observable<string>();
    disabled = ko.observable<boolean>(false);

    isNew = ko.observable<boolean>(true);
    isSelected = ko.observable<boolean>(false);

    columns = ko.observableArray<cdcSinkColumnMapping>([]);
    primaryKeyColumns = ko.observableArray<string>([]);

    displayName: KnockoutComputed<string>;

    validationGroup: KnockoutValidationGroup;
    testValidationGroup: KnockoutValidationGroup;

    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.CdcSink.CdcSinkTableConfig, isNew: boolean) {
        this.update(dto, isNew);
        this.initObservables();
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
                this.name,
                this.sourceTableSchema,
                this.sourceTableName,
                this.patch,
                this.disabled,
                this.isSelected,
            ],
            false, jsonUtil.newLineNormalizingHashFunction);
    }

    static empty(name?: string): ongoingTaskCdcSinkTableModel {
        return new ongoingTaskCdcSinkTableModel(
            {
                Name: name || "",
                SourceTableSchema: "public",
                SourceTableName: "",
                Columns: [],
                PrimaryKeyColumns: [],
                Patch: "",
                Disabled: false,
                EmbeddedTables: [],
                LinkedTables: [],
                OnDelete: null,
            }, true);
    }

    static fromSchemaTable(tableSchema: Raven.Server.SqlMigration.Schema.SqlTableSchema): ongoingTaskCdcSinkTableModel {
        const collectionName = ongoingTaskCdcSinkTableModel.tableNameToCollectionName(tableSchema.TableName);

        const columns = tableSchema.Columns.map(col => ({
            Column: col.Name,
            Name: col.Name,
            Type: "Default" as Raven.Client.Documents.Operations.CdcSink.CdcColumnType,
        }));

        const model = new ongoingTaskCdcSinkTableModel({
            Name: collectionName,
            SourceTableSchema: tableSchema.Schema || "public",
            SourceTableName: tableSchema.TableName,
            Columns: columns,
            PrimaryKeyColumns: tableSchema.PrimaryKeyColumns || [],
            Patch: "",
            Disabled: false,
            EmbeddedTables: [],
            LinkedTables: [],
            OnDelete: null,
        }, true);

        model.isSelected(true);

        const columnMappings = tableSchema.Columns.map(col => {
            const isPk = (tableSchema.PrimaryKeyColumns || []).indexOf(col.Name) >= 0;
            return new cdcSinkColumnMapping(col.Name, col.Type, col.Name, isPk);
        });
        model.columns(columnMappings);
        model.primaryKeyColumns(tableSchema.PrimaryKeyColumns || []);

        return model;
    }

    private static tableNameToCollectionName(tableName: string): string {
        // Convert snake_case to PascalCase and pluralize
        return tableName.split("_")
            .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
            .join("");
    }

    toDto(): Raven.Client.Documents.Operations.CdcSink.CdcSinkTableConfig {
        const columns: Raven.Client.Documents.Operations.CdcSink.CdcColumnMapping[] = this.columns().map(col => ({
            Column: col.sqlColumnName,
            Name: col.propertyName(),
            Type: "Default" as Raven.Client.Documents.Operations.CdcSink.CdcColumnType,
        }));

        return {
            Name: this.name(),
            SourceTableSchema: this.sourceTableSchema(),
            SourceTableName: this.sourceTableName(),
            Columns: columns,
            PrimaryKeyColumns: this.primaryKeyColumns(),
            Patch: this.patch(),
            Disabled: this.disabled(),
            EmbeddedTables: [],
            LinkedTables: [],
            OnDelete: null,
        };
    }

    private initObservables() {
        this.displayName = ko.pureComputed(() => {
            const schema = this.sourceTableSchema();
            const table = this.sourceTableName();
            if (schema && schema !== "public" && schema !== "dbo") {
                return schema + "." + table;
            }
            return table;
        });
    }

    private initValidation() {
        this.name.extend({
            required: true
        });

        this.sourceTableName.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            name: this.name,
            sourceTableName: this.sourceTableName,
        });

        this.testValidationGroup = ko.validatedObservable({
            name: this.name,
            sourceTableName: this.sourceTableName,
        });
    }

    private update(dto: Raven.Client.Documents.Operations.CdcSink.CdcSinkTableConfig, isNew: boolean) {
        this.name(dto.Name);
        this.sourceTableSchema(dto.SourceTableSchema);
        this.sourceTableName(dto.SourceTableName);
        this.patch(dto.Patch || "");
        this.disabled(dto.Disabled || false);
        this.isNew(isNew);
        this.primaryKeyColumns(dto.PrimaryKeyColumns || []);

        const dtoColumns = dto.Columns || [];
        if (dtoColumns.length > 0) {
            const pkCols = dto.PrimaryKeyColumns || [];
            const mappings = dtoColumns.map(col => {
                const isPk = pkCols.indexOf(col.Column) >= 0;
                return new cdcSinkColumnMapping(col.Column, col.Type || "Default", col.Name, isPk);
            });
            this.columns(mappings);
        }
    }

    hasUpdates(oldItem: this) {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    }
}

export = ongoingTaskCdcSinkTableModel;
