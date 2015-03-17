import app = require("durandal/app");
import database = require("models/database");
import testSqlConnectionCommand = require("commands/testSqlConnectionCommand");

class predefinedSqlConnection {
    name=ko.observable<string>();
    factoryName = ko.observable<string>();
    connectionString = ko.observable<string>();
    connectionTestState = ko.observable<string>(this.CONNECTION_STATE_STAND_BY);

    hasGlobal = ko.observable<boolean>(false);
    hasLocal = ko.observable<boolean>(true);

    globalConfiguration = ko.observable<predefinedSqlConnection>();

    canEdit = ko.computed(() => this.hasLocal());

    public CONNECTION_STATE_STAND_BY = "stand-by";
    public CONNECTION_STATE_CONNECTING = "connecting";
    public CONNECTION_STATE_CONNECTED = "connected";

    constructor(dto: predefinedSqlConnectionDto) {
        this.name(dto.Name);
        this.factoryName(dto.FactoryName);
        this.connectionString(dto.ConnectionString);
        this.connectionTestState = ko.observable<string>(this.CONNECTION_STATE_STAND_BY);
        this.hasGlobal(dto.HasGlobal);
        this.hasLocal(dto.HasLocal);
    }

    static empty(): predefinedSqlConnection {
        return new predefinedSqlConnection({
            Name:"",
            FactoryName: "",
            ConnectionString: "",
            HasGlobal: false,
            HasLocal: true
        });
    }

    toDto(): predefinedSqlConnectionDto {
        return {
            Name:this.name(),
            FactoryName: this.factoryName(),
            ConnectionString: this.connectionString()
        }
    }

    testConnection(db: database) {
        this.connectionTestState(this.CONNECTION_STATE_CONNECTING);
        new testSqlConnectionCommand(db, this.factoryName(), this.connectionString())
            .execute()
            .done(() => {
                this.connectionTestState(this.CONNECTION_STATE_CONNECTED);
                app.showMessage("Connection " + this.name() + " is valid", "SQL Connection test");
            })
            .fail((request, status, error) => {
                var errorText = !!request.responseJSON ? !!request.responseJSON.Exception ? request.responseJSON.Exception.Message : error : error;
                app.showMessage("Connection " + this.name() + " is not valid, error: " + errorText, "SQL Connection test");
            }).
            always(() => setTimeout(this.connectionTestState, 500, this.CONNECTION_STATE_STAND_BY ));
    }

    copyFromGlobal() {
        if (this.globalConfiguration()) {
            var gConfig = this.globalConfiguration();

            this.name(gConfig.name());
            this.factoryName(gConfig.factoryName());
            this.connectionString(gConfig.connectionString());
            this.connectionTestState(gConfig.connectionTestState());

            this.hasGlobal(true);
            this.hasLocal(false);
        }
    }
}

export =predefinedSqlConnection;