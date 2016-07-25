import sqlReplicationTable = require("models/database/sqlReplication/sqlReplicationTable");
import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class sqlReplication extends document {

    CONNECTION_STRING = "Connection String";
    PREDEFINED_CONNECTION_STRING_NAME = "Predefined Connection String Name";
    CONNECTION_STRING_NAME = "Connection String Name";
    CONNECTION_STRING_SETTING_NAME = "Connection String Setting Name";
    
    availableConnectionStringTypes = [
        this.PREDEFINED_CONNECTION_STRING_NAME,
        this.CONNECTION_STRING ,
        this.CONNECTION_STRING_NAME,
        this.CONNECTION_STRING_SETTING_NAME 
    ];

    metadata: documentMetadata;

    name = ko.observable<string>().extend({ required: true });
    disabled = ko.observable<boolean>().extend({ required: true });
    factoryName = ko.observable<string>().extend({ required: true });
    connectionStringType = ko.observable<string>().extend({ required: true });
    connectionStringValue = ko.observable<string>(null).extend({ required: true });
    ravenEntityName = ko.observable<string>("").extend({ required: true });
    parameterizeDeletesDisabled = ko.observable<boolean>(false).extend({ required: true });
    forceSqlServerQueryRecompile = ko.observable<boolean>(false);
    quoteTables = ko.observable<boolean>(true);
    sqlReplicationTables = ko.observableArray<sqlReplicationTable>().extend({ required: true });
    script = ko.observable<string>("").extend({ required: true });
    connectionString = ko.observable<string>(null);
    connectionStringName = ko.observable<string>(null);
    connectionStringSettingName = ko.observable<string>(null);
    connectionStringSourceFieldName: KnockoutComputed<string>;
    
    collections = ko.observableArray<string>();
    searchResults: KnockoutComputed<string[]>;
    isVisible = ko.observable<boolean>(true);
    
    showReplicationConfiguration = ko.observable<boolean>(false);

    hasAnyInsertOnlyOption = ko.computed(() => {
        var hasAny = false;
        this.sqlReplicationTables().forEach(s => {
            // don't use return here to register all deps in knockout
            if (s.insertOnly()) {
                hasAny = true;
            }
        });
        return hasAny;
    });

    constructor(dto: sqlReplicationDto) {
        super(dto);

        this.name(dto.Name);
        this.disabled(dto.Disabled);
        this.factoryName(dto.FactoryName);
        this.ravenEntityName(dto.RavenEntityName != null ? dto.RavenEntityName : "");
        this.parameterizeDeletesDisabled(dto.ParameterizeDeletesDisabled);
        this.sqlReplicationTables(dto.SqlReplicationTables.map(tab => new sqlReplicationTable(tab)));
        this.script(dto.Script);
        this.forceSqlServerQueryRecompile(!!dto.ForceSqlServerQueryRecompile? dto.ForceSqlServerQueryRecompile:false);
        this.quoteTables(("PerformTableQuatation" in dto) ? dto.PerformTableQuatation : ("QuoteTables" in dto ? dto.QuoteTables : true));
        this.setupConnectionString(dto);

        this.metadata = new documentMetadata(dto["@metadata"]);

        this.connectionStringSourceFieldName = ko.computed(() => {
            if (this.connectionStringType() == this.CONNECTION_STRING) {
                return "Connection String Text";
            } else if (this.connectionStringType() == this.PREDEFINED_CONNECTION_STRING_NAME) {
                return "Predefined connection string name";
            } else if (this.connectionStringType() == this.CONNECTION_STRING_NAME) {
                return "Setting name in local machine configuration";
            } else {
                return "Setting name in memory/remote configuration";
            }
        });

        this.searchResults = ko.computed(() => {
            var newRavenEntityName: string = this.ravenEntityName();
            return this.collections().filter((name) => name.toLowerCase().indexOf(newRavenEntityName.toLowerCase()) > -1);
        });
        
        this.script.subscribe((newValue) => {
            var message = "";
            var currentEditor = aceEditorBindingHandler.currentEditor;
            var textarea: any = $(currentEditor.container).find("textarea")[0];

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
        } else if (dto.ConnectionString){
            this.connectionStringType(this.CONNECTION_STRING);
            this.connectionStringValue(dto.ConnectionString);
        }
        else {
            this.connectionStringType(this.PREDEFINED_CONNECTION_STRING_NAME);
            this.connectionStringValue(dto.PredefinedConnectionStringSettingName);
        }
    }

    setConnectionStringType(strType: string) {
        this.connectionStringType(strType);
    }

    static empty(): sqlReplication {
        return new sqlReplication({
            Name: "",
            Disabled: true,
            ParameterizeDeletesDisabled: false,
            RavenEntityName: "",
            Script: "",
            FactoryName: null,
            ConnectionString: null,
            PredefinedConnectionStringSettingName:null,
            ConnectionStringName: null,
            ConnectionStringSettingName: null,
            SqlReplicationTables: [sqlReplicationTable.empty().toDto()],
            ForceSqlServerQueryRecompile: false,
            QuoteTables:true
        });
    }

    toDto(): sqlReplicationDto {
        var meta = this.__metadata.toDto();
        meta["@id"] = "Raven/SqlReplication/Configuration/" + this.name();
        return {
            '@metadata': meta,
            Name: this.name(),
            Disabled: this.disabled(),
            ParameterizeDeletesDisabled: this.parameterizeDeletesDisabled(),
            RavenEntityName: this.ravenEntityName(),
            Script: this.script(),
            FactoryName: this.factoryName(),
            ConnectionString: this.prepareConnectionString(this.CONNECTION_STRING),
            PredefinedConnectionStringSettingName: this.prepareConnectionString(this.PREDEFINED_CONNECTION_STRING_NAME),
            ConnectionStringName: this.prepareConnectionString(this.CONNECTION_STRING_NAME),
            ConnectionStringSettingName: this.prepareConnectionString(this.CONNECTION_STRING_SETTING_NAME),
            ForceSqlServerQueryRecompile: this.forceSqlServerQueryRecompile(),
            QuoteTables: this.quoteTables(),
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

    saveNewRavenEntityName(newRavenEntityName: string) {
        this.ravenEntityName(newRavenEntityName);
    }


    isSqlServerKindOfFactory(factoryName:string): boolean {
        if (factoryName == "System.Data.SqlClient" || factoryName == "System.Data.SqlServerCe.4.0" || factoryName == "System.Data.SqlServerCe.3.5") {
            return true;
        }
        return false;
    }
}

export = sqlReplication;
