/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");

class discoveryUrl {
    discoveryUrlName = ko.observable<string>();
    validationGroup: KnockoutValidationGroup;
    
    constructor(urlName: string) {      

        this.discoveryUrlName(urlName);
        this.initValidation();
    }
    
    initValidation() {
        this.discoveryUrlName.extend({         
            validUrl: true
        });

        this.validationGroup = ko.validatedObservable({
            discoveryUrlName: this.discoveryUrlName
        });
    }
}

class connectionStringRavenEtlModel extends connectionStringModel { 

    database = ko.observable<string>();
    topologyDiscoveryUrls = ko.observableArray<discoveryUrl>([]);    
    inputUrl = ko.observable<discoveryUrl>(new discoveryUrl(""));
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
        this.topologyDiscoveryUrls(dto.TopologyDiscoveryUrls.map((x) => new discoveryUrl(x)));        
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
        });     
       
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
            TopologyDiscoveryUrls: this.topologyDiscoveryUrls().map((x) => x.discoveryUrlName()),
            Database: this.database()
        };
    }
    
    removeDiscoveryUrl(url: discoveryUrl) {
        this.topologyDiscoveryUrls.remove(url); 
    }   

    addDiscoveryUrlWithBlink() { 
        if ( !_.includes(this.topologyDiscoveryUrls(), this.inputUrl())) {
            this.topologyDiscoveryUrls.unshift(new discoveryUrl(this.inputUrl().discoveryUrlName()));
            this.inputUrl().discoveryUrlName("");

            $(".collection-list li").first().addClass("blink-style");
        }
    }

    testConnection(urlToTest: string) : JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {       
        return new testClusterNodeConnectionCommand(urlToTest)
            .execute();
    }
}

export = connectionStringRavenEtlModel;
