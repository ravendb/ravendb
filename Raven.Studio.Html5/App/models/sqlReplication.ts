import sqlReplicationTable = require("models/sqlReplicationTable");
import document = require("models/document");
import documentMetadata = require("models/documentMetadata");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");

class sqlReplication extends document {

    private CONNECTION_STRING = "Connection String";
    private CONNECTION_STRING_NAME = "Connection String Name";
    private CONNECTION_STRING_SETTING_NAME = "Connection String Setting Name";
    
    availableConnectionStringTypes = [
        { label: "Connection String", value: this.CONNECTION_STRING },
        { label: "Connection String Name", value: this.CONNECTION_STRING_NAME },
        { label: "Connection String Setting Name", value: this.CONNECTION_STRING_SETTING_NAME }
    ];

    public metadata: documentMetadata;

    name = ko.observable<string>().extend({ required: true });
    disabled = ko.observable<boolean>().extend({ required: true });
    factoryName = ko.observable<string>().extend({ required: true });
    connectionStringType = ko.observable<string>().extend({ required: true });
    connectionStringValue = ko.observable<string>(null).extend({ required: true });
    ravenEntityName = ko.observable<string>("").extend({ required: true });
    parameterizeDeletesDisabled = ko.observable<boolean>(false).extend({ required: true });
    forceSqlServerQueryRecompile = ko.observable<boolean>(false);
    sqlReplicationTables = ko.observableArray<sqlReplicationTable>().extend({ required: true });
    script = ko.observable<string>("").extend({ required: true });
    connectionString = ko.observable<string>(null);
    connectionStringName = ko.observable<string>(null);
    connectionStringSettingName = ko.observable<string>(null);
    connectionStringSourceFieldName: KnockoutComputed<string>;

    collections = ko.observableArray<string>();
    searchResults = ko.observableArray<string>();

    showReplicationConfiguration = ko.observable<boolean>(false);

    constructor(dto: sqlReplicationDto) {
        super(dto);

        this.name(dto.Name);
        this.disabled(dto.Disabled);
        this.factoryName(dto.FactoryName);
        this.ravenEntityName(dto.RavenEntityName);
        this.parameterizeDeletesDisabled(dto.ParameterizeDeletesDisabled);
        this.sqlReplicationTables(dto.SqlReplicationTables.map(tab => new sqlReplicationTable(tab)));
        this.script(dto.Script);
        this.forceSqlServerQueryRecompile(!!dto.ForceSqlServerQueryRecompile? dto.ForceSqlServerQueryRecompile:false);

        this.setupConnectionString(dto);

        this.metadata = new documentMetadata(dto['@metadata']);

        this.connectionStringSourceFieldName = ko.computed(() => {
            if (this.connectionStringType() == this.CONNECTION_STRING) {
                return "Connection String Text";
            } else if (this.connectionStringType() == this.CONNECTION_STRING_NAME) {
                return "Setting name in local machine configuration";
            } else {
                return "Setting name in memory/remote configuration";
            }
        });

        this.ravenEntityName.subscribe((newRavenEntityName) => {
            this.searchResults(this.collections().filter((name) => {
                return !!newRavenEntityName && name.toLowerCase().indexOf(newRavenEntityName.toLowerCase()) > -1;
            }));

        });

        this.script.subscribe((newValue) => {
            var message = "";
            var currentEditor = aceEditorBindingHandler.currentEditor;
            var textarea: any = $(currentEditor.container).find('textarea')[0];

            if (newValue === "") {
                message = "Please fill out this field.";
            }
            textarea.setCustomValidity(message);
            setTimeout(() => {
                var annotations = currentEditor.getSession().getAnnotations();
                var isErrorExists = false;
                for (var i = 0; i < annotations.length; i++) {
                    var annotationType = annotations[i].type;
                    if (annotationType === "error" || annotationType === "warning") {
                        isErrorExists = true;
                        break;
                    }
                }
                if (isErrorExists) {
                    message = "The script isn't a javascript legal expression!";
                    textarea.setCustomValidity(message);
                }
            }, 700);
        });
    }

    private setupConnectionString(dto: sqlReplicationDto) {
        if (dto.ConnectionStringName) {
            this.connectionStringType(this.CONNECTION_STRING_NAME);
            this.connectionStringValue(dto.ConnectionStringName);
        } else if (dto.ConnectionStringSettingName) {
            this.connectionStringType(this.CONNECTION_STRING_SETTING_NAME);
            this.connectionStringValue(dto.ConnectionStringSettingName);
        } else { //(dto.ConnectionString)
            this.connectionStringType(this.CONNECTION_STRING);
            this.connectionStringValue(dto.ConnectionString);
        }
    }

    setConnectionStringType(strType) {
        this.connectionStringType(strType);
    }

    static empty(): sqlReplication {
        var newTable: sqlReplicationTable = sqlReplicationTable.empty();
        var sqlReplicationTables = [];
        sqlReplicationTables.push(newTable);
        return new sqlReplication({
            Name: "",
            Disabled: true,
            ParameterizeDeletesDisabled: false,
            RavenEntityName: "",
            Script: "",
            FactoryName: null,
            ConnectionString: null,
            ConnectionStringName: null,
            ConnectionStringSettingName: null,
            SqlReplicationTables: sqlReplicationTables,
            ForceSqlServerQueryRecompile:false
        });
    }

    toDto(): sqlReplicationDto {
        var meta = this.__metadata.toDto();
        meta['@id'] = "Raven/SqlReplication/Configuration/" + this.name();
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
            ForceSqlServerQueryRecompile: this.forceSqlServerQueryRecompile(),
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

    enableParameterizeDeletes() {
        this.parameterizeDeletesDisabled(false);
    }

    disableParameterizeDeletes() {
        this.parameterizeDeletesDisabled(true);
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

    saveNewRavenEntityName(newRavenEntityName) {
        this.ravenEntityName(newRavenEntityName);
    }


    isSqlServerKindOfDB(): boolean {
        if (this.factoryName() == 'System.Data.SqlClient' || this.factoryName() == 'System.Data.SqlServerCe.4.0' || this.factoryName() == 'System.Data.SqlServerCe.3.5') {
            return true;
        }
        return false;
    }
}

export = sqlReplication;