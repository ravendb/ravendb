/// <reference path="../models/dto.ts" />

import document = require("models/document");
import predefinedConnection = require("models/predefinedSqlConnection");

class sqlReplicationConnections extends document {

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

    constructor(dto: configurationDocumentDto<sqlReplicationConnectionsDto>) {
        super(dto);

        this.predefinedConnections(dto.MergedDocument.PredefinedConnections.map(c => {
            var result = new predefinedConnection(c);
            if (dto.GlobalDocument) {
                var foundParent = dto.GlobalDocument.PredefinedConnections.first(x => x.Name.toLowerCase() === c.Name.toLowerCase());
                if (foundParent) {
                    result.globalConfiguration(new predefinedConnection(foundParent));
                }
            }
            return result;
        }));
    }

    static empty(): sqlReplicationConnections {
        return new sqlReplicationConnections({
            MergedDocument: {
                PredefinedConnections: []
            },
            LocalExists: true,
            GlobalExists: false
        });
    }
   
    toDto(filterLocal = true): sqlReplicationConnectionsDto {
        var meta = this.__metadata.toDto();
        meta["@id"] = "Raven/SqlReplication/Connections/";
        return {
            PredefinedConnections: this.predefinedConnections().filter(dest => !filterLocal || dest.hasLocal()).map(x => x.toDto()) 
        };
    }

    copyFromParent() {
        this.predefinedConnections(this.predefinedConnections().filter(c => c.hasGlobal()));
        this.predefinedConnections().forEach(c => c.copyFromGlobal());
    }
}

export =sqlReplicationConnections;