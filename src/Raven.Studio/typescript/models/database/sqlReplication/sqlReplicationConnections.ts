/// <reference path="../../../../typings/tsd.d.ts" />

import document = require("models/database/documents/document");
import predefinedConnection = require("models/database/sqlReplication/predefinedSqlConnection");
import documentMetadata = require("models/database/documents/documentMetadata");

class sqlReplicationConnections {

    static sqlProvidersConnectionStrings: { ProviderName: string; ConnectionString: string; }[] = [
        { ProviderName: "System.Data.SqlClient", ConnectionString: "Server=[Server Address];Database=[Database Name];User Id=[User ID];Password=[Password];" },
        { ProviderName: "System.Data.SqlServerCe.4.0", ConnectionString: "Data Source=[path of .sdf file];Persist Security Info=False;" },
        { ProviderName: "System.Data.SqlServerCe.3.5", ConnectionString: "Provider=Microsoft.SQLSERVER.CE.OLEDB.3.5;Data Source=[path of .sdf file];" },
        { ProviderName: "System.Data.OleDb", ConnectionString: "" },
        { ProviderName: "System.Data.OracleClient", ConnectionString: "Data Source=[TNSNames name];User Id=[User ID];Password=[Password];Integrated Security=no;" },
        { ProviderName: "MySql.Data.MySqlClient", ConnectionString: "Server=[Server Address];Port=[Server Port(default:3306)];Database=[Database Name];Uid=[User ID];Pwd=[Password];" },
        { ProviderName: "Npgsql", ConnectionString: "Server=[Server Address];Port=[Port];Database=[Database Name];User Id=[User ID];Password=[Password];" }
    ];

    predefinedConnections = ko.observableArray<predefinedConnection>();

    constructor(dto: Raven.Server.Documents.SqlReplication.SqlConnections) {

        //TODO: this.__metadata = dto["metadata"];
        if (dto) {
            const connections = [] as Array<predefinedConnection>;
            for (const connectionName in dto.Connections) {
                connections.push(new predefinedConnection(connectionName, dto.Connections[connectionName]));
            }

            this.predefinedConnections(connections);    
        }
    }

    static empty(): sqlReplicationConnections {
        return new sqlReplicationConnections({
            Connections: {},
            Id: null
        } as Raven.Server.Documents.SqlReplication.SqlConnections);
    }
   
    toDto(): Raven.Server.Documents.SqlReplication.SqlConnections {
        //TODO: var meta = this.__metadata.toDto();
        //TODO: meta["@id"] = "Raven/SqlReplication/Connections/";
        const connectionsMap = {} as System.Collections.Generic.
            Dictionary<string, Raven.Server.Documents.SqlReplication.PredefinedSqlConnection>;

        this.predefinedConnections().forEach(c => {
            connectionsMap[c.name()] = c.toDto();
        });

        return {
            Connections: connectionsMap,
            Id: null
        };
    }

}

export =sqlReplicationConnections;
