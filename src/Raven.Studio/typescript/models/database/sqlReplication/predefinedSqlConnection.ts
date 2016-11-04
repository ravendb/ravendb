import app = require("durandal/app");
import database = require("models/resources/database");
import testSqlConnectionCommand = require("commands/database/sqlReplication/testSqlConnectionCommand");

class predefinedSqlConnection {
    name = ko.observable<string>();
    factoryName = ko.observable<string>();
    connectionString = ko.observable<string>();
    connectionTestState = ko.observable<string>(this.CONNECTION_STATE_STAND_BY);

    public CONNECTION_STATE_STAND_BY = "stand-by";
    public CONNECTION_STATE_CONNECTING = "connecting";
    public CONNECTION_STATE_CONNECTED = "connected";

    constructor(name: string, dto: Raven.Server.Documents.SqlReplication.PredefinedSqlConnection) {
        this.name(name);
        this.factoryName(dto.FactoryName);
        this.connectionString(dto.ConnectionString);
        this.connectionTestState = ko.observable<string>(this.CONNECTION_STATE_STAND_BY);
    }

    static empty(): predefinedSqlConnection {
        return new predefinedSqlConnection("", {
            FactoryName: "",
            ConnectionString: ""
        });
    }

    toDto(): Raven.Server.Documents.SqlReplication.PredefinedSqlConnection {
        return {
            FactoryName: this.factoryName(),
            ConnectionString: this.connectionString()
        }
    }

    testConnection(dbObservable: KnockoutObservable<database>) {
        this.connectionTestState(this.CONNECTION_STATE_CONNECTING);
        new testSqlConnectionCommand(dbObservable(), this.factoryName(), this.connectionString())
            .execute()
            .done(() => {
                this.connectionTestState(this.CONNECTION_STATE_CONNECTED);
                app.showBootstrapMessage("Connection " + this.name() + " is valid", "SQL Connection test");
            })
            .fail((request, status, error) => {
                var errorText = !!request.responseJSON ? !!request.responseJSON.Exception ? request.responseJSON.Exception.Message : error : error;
                app.showBootstrapMessage("Connection " + this.name() + " is not valid, error: " + errorText, "SQL Connection test");
            }).
            always(() => setTimeout(this.connectionTestState, 500, this.CONNECTION_STATE_STAND_BY ));
    }

}

export =predefinedSqlConnection;
