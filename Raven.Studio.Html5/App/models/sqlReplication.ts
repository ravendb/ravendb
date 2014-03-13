import sqlReplicationTable = require("models/sqlReplicationTable");

class sqlReplication {

    private CONNECTION_STRING = "cs";
    private CONNECTION_STRING_NAME = "csn";
    private CONNECTION_STRING_SETTING_NAME = "cssn";

    availableConnectionStringTypes = [
        { label: "Connection String", value: this.CONNECTION_STRING },
        { label: "Connection String Name", value: this.CONNECTION_STRING_NAME },
        { label: "Connection String Setting Name", value: this.CONNECTION_STRING_SETTING_NAME }
    ];

    name = ko.observable<string>();
    disabled = ko.observable<boolean>();
    parameterizeDeletesDisabled = ko.observable<boolean>();
    ravenEntityName = ko.observable<string>();
    script = ko.observable<string>();
    factoryName = ko.observable<string>();
    connectionString = ko.observable<string>();
    connectionStringName = ko.observable<string>();
    connectionStringSettingName = ko.observable<string>();
    sqlReplicationTables = ko.observableArray<sqlReplicationTable>();

    constructor(dto: sqlReplicationDto) {
        this.name(dto.Name);
        this.disabled(dto.Disabled);
        this.parameterizeDeletesDisabled(dto.ParameterizeDeletesDisabled);
        this.ravenEntityName(dto.RavenEntityName);
        this.script(dto.Script);
        this.factoryName(dto.FactoryName);
        this.connectionString(dto.ConnectionString);
        this.connectionStringName(dto.ConnectionStringName);
        this.connectionStringSettingName(dto.ConnectionStringSettingName);
        this.sqlReplicationTables(dto.SqlReplicationTables.map(tab => new sqlReplicationTable(tab)));
    }

    static empty(): sqlReplication {
        return new sqlReplication({
            Name: "[new SQL replication]",
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
        return {
            Name: this.name(),
            Disabled: this.disabled(),
            ParameterizeDeletesDisabled: this.parameterizeDeletesDisabled(),
            RavenEntityName: this.ravenEntityName(),
            Script: this.script(),
            FactoryName: this.factoryName(),
            ConnectionString: this.connectionString(),
            ConnectionStringName: this.connectionStringName(),
            ConnectionStringSettingName: this.connectionStringSettingName(),
            SqlReplicationTables: this.sqlReplicationTables().map(tab => tab.toDto())
        };
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
}

export = sqlReplication;