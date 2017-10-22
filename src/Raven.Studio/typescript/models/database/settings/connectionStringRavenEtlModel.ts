/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");

class connectionStringRavenEtlModel extends connectionStringModel { 

    database = ko.observable<string>();
    topologyDiscoveryUrls = ko.observableArray<string>([]);     
    
    inputUrl = ko.observable<string>();
    selectedUrlToTest = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;
    
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
            validation: [
                {
                    validator: () => this.topologyDiscoveryUrls().length > 0,
                    message: "All least one discovery url is required"
                }
            ]
        });      // TODO: How to validate each url in the list with 'ValidUrl' ?      
       
        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            database: this.database,
            topologyDiscoveryUrls: this.topologyDiscoveryUrls
        });
    }

    static empty(): connectionStringRavenEtlModel {
        return new connectionStringRavenEtlModel({
            Type: "Raven",
            Name: "", 
            TopologyDiscoveryUrls: [],
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
    
    removeDiscoveryUrl(url: string) {
        this.topologyDiscoveryUrls.remove(url);
    }   

    addDiscoveryUrlWithBlink() {
        if ( !_.includes(this.topologyDiscoveryUrls(), this.inputUrl())) {
            this.topologyDiscoveryUrls.unshift(this.inputUrl());
            this.inputUrl("");

            $(".collection-list li").first().addClass("blink-style");
        }
    }

    testConnection(urlToTest: string) : JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {       
        return new testClusterNodeConnectionCommand(urlToTest)
            .execute();
    }
}

export = connectionStringRavenEtlModel;
