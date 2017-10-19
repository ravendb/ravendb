/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");

class connectionStringRavenEtlModel extends connectionStringModel { 

    topologyDiscoveryUrls = ko.observable<string[]>();                 
    database = ko.observable<string>();            

    validationGroup: KnockoutValidationGroup;
    testConnectionValidationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.ServerWide.ETL.RavenConnectionString, isNew: boolean, tasks: string[]) {
        super(isNew, tasks);
        
        this.update(dto);       
        this.initValidation();      
    }    

    update(dto: Raven.Client.ServerWide.ETL.RavenConnectionString) {
        super.update(dto);
        
        this.connectionStringName(dto.Name); 
        this.database(dto.Database);
        this.topologyDiscoveryUrls(dto.TopologyDiscoveryUrls);
    }

    initValidation() {
        super.initValidation();
        
        this.connectionStringName.extend({
            required: true
        });

        this.database.extend({
            required: true,
            validDatabaseName: true            
        });

        this.topologyDiscoveryUrls.extend({
            required: true,
            validUrl: true
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            database: this.database,
            topologyDiscoveryUrls: this.topologyDiscoveryUrls
        });
        
        this.testConnectionValidationGroup = ko.validatedObservable({
            topologyDiscoveryUrls: this.topologyDiscoveryUrls
        })
    }

    static empty(): connectionStringRavenEtlModel {
        return new connectionStringRavenEtlModel({
            Type: "Raven",
            Name: "", 
            TopologyDiscoveryUrls: null,
            Database: ""
        } as Raven.Client.ServerWide.ETL.RavenConnectionString, true, []);
    }
    
    toDto() {
        return {
            Type: "Raven",
            Name: this.connectionStringName(),
            TopologyDiscoveryUrls: this.topologyDiscoveryUrls(),
            Database: this.database()
        };
    }
}

export = connectionStringRavenEtlModel;
