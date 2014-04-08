import sqlReplicationTable = require("models/sqlReplicationTable");
import document = require("models/document");
import documentMetadata = require("models/documentMetadata");

class sqlReplication extends document {

    private CONNECTION_STRING = "cs";
    private CONNECTION_STRING_NAME = "csn";
    private CONNECTION_STRING_SETTING_NAME = "cssn";

    availableConnectionStringTypes = [
        { label: "Connection String", value: this.CONNECTION_STRING },
        { label: "Connection String Name", value: this.CONNECTION_STRING_NAME },
        { label: "Connection String Setting Name", value: this.CONNECTION_STRING_SETTING_NAME }
    ];

    public metadata: documentMetadata;

    name = ko.observable<string>().extend({ required: true });
    disabled = ko.observable<boolean>().extend({ required: true });
    parameterizeDeletesDisabled = ko.observable<boolean>();
    ravenEntityName = ko.observable<string>('').extend({ required: true });
    script = ko.observable<string>().extend({ required: true });
    factoryName = ko.observable<string>().extend({ required: true });
    connectionString = ko.observable<string>().extend({ required: true });
    connectionStringName = ko.observable<string>().extend({ required: true });
    connectionStringSettingName = ko.observable<string>().extend({ required: true });
    sqlReplicationTables = ko.observableArray<sqlReplicationTable>().extend({ required: true });

    connectionStringType = ko.observable<string>().extend({ required: true });
    connectionStringValue = ko.observable<string>('').extend({ required: true });

    constructor(dto: sqlReplicationDto) {
        super(dto);

        this.name(dto.Name);
        this.disabled(dto.Disabled);
        this.parameterizeDeletesDisabled(dto.ParameterizeDeletesDisabled);
        this.ravenEntityName(dto.RavenEntityName);
        this.script(dto.Script);
        this.factoryName(dto.FactoryName);
        this.sqlReplicationTables(dto.SqlReplicationTables.map(tab => new sqlReplicationTable(tab)));

        this.setupConnectionString(dto);

        this.metadata = new documentMetadata(dto['@metadata']);
    }

    private setupConnectionString(dto: sqlReplicationDto) {
        if (dto.ConnectionString) {
            this.connectionStringType(this.CONNECTION_STRING);
            this.connectionStringValue(dto.ConnectionString);
        } else if (dto.ConnectionStringName) {
            this.connectionStringType(this.CONNECTION_STRING_NAME);
            this.connectionStringValue(dto.ConnectionStringName);
        } else if (dto.ConnectionStringSettingName) {
            this.connectionStringType(this.CONNECTION_STRING_SETTING_NAME);
            this.connectionStringValue(dto.ConnectionStringSettingName);
        }
    }

    static empty(): sqlReplication {
        return new sqlReplication({
            Name: "",
            Disabled: true,
            ParameterizeDeletesDisabled: false,
            RavenEntityName: null,
            Script: null,
            FactoryName: null,
            ConnectionString: null,
            ConnectionStringName: null,
            ConnectionStringSettingName: null,
            SqlReplicationTables: []
        });
    }

    toDto(): sqlReplicationDto {
        var meta = this.__metadata.toDto();
        meta['@id'] = "Raven/ApiKeys/" + this.name();
        return {
            '@metadata': meta,
            Name: this.name(),
            Disabled: this.disabled(),
            ParameterizeDeletesDisabled: this.parameterizeDeletesDisabled(),
            RavenEntityName: this.ravenEntityName(),
            Script: this.script(),
            FactoryName: this.factoryName(),
            ConnectionString: this.prepareConnectionString(this.CONNECTION_STRING),
            ConnectionStringName: this.prepareConnectionString(this.CONNECTION_STRING_NAME),
            ConnectionStringSettingName: this.prepareConnectionString(this.CONNECTION_STRING_SETTING_NAME),
            SqlReplicationTables: this.sqlReplicationTables().map(tab => tab.toDto())
        };
    }

    private prepareConnectionString(expectedType: string): string {
        return ((this.connectionStringType() === expectedType) ? this.connectionStringValue() : null);
    }

    enable() {
        this.disabled(false);
    }

    disable() {
        this.disabled(true);
    }

    addNewTable() {
        this.sqlReplicationTables.push(sqlReplicationTable.empty());
    }

    removeTable(table: sqlReplicationTable) {
        this.sqlReplicationTables.remove(table);
    }

    setIdFromName() {
        this.__metadata.id = "Raven/SqlReplication/Configuration/" + this.name();
    }
}

export = sqlReplication;