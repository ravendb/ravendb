/// <reference path="../models/dto.ts" />

import document = require("models/document");
import predefinedConnection = require("models/predefinedSqlConnection");

class sqlReplicationConnections extends document {

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
        meta['@id'] = "Raven/SqlReplication/Connections/";
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