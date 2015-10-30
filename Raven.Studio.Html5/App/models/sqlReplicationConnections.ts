/// <reference path="../models/dto.ts" />

import document = require("models/document");
import documentMetadata = require("models/documentMetadata");
import predefinedConnection = require("models/predefinedSqlConnection");

class sqlReplicationConnections extends document {

    predefinedConnections = ko.observableArray<predefinedConnection>();


    constructor(dto: sqlReplicationConnectionsDto) {
        super(dto);
        this.predefinedConnections(dto.PredefinedConnections.map(x => new predefinedConnection(x)));

    }

    static empty(): sqlReplicationConnections {
        return new sqlReplicationConnections({
            PredefinedConnections:[]
        });
    }
   
    toDto(): sqlReplicationConnectionsDto {
        var meta = this.__metadata.toDto();
        meta['@id'] = "Raven/SqlReplication/Connections/";
        return {
            PredefinedConnections: this.predefinedConnections().map(x=>x.toDto()) 
        };
    }

    

}

export =sqlReplicationConnections;
