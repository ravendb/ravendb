/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");
import jsonUtil = require("common/jsonUtil");
import discoveryUrl = require("models/database/settings/discoveryUrl");


class connectionStringRavenEtlModel extends connectionStringModel { 

    database = ko.observable<string>();
    topologyDiscoveryUrls = ko.observableArray<discoveryUrl>([]);    
    inputUrl = ko.observable<discoveryUrl>(new discoveryUrl(""));
    selectedUrlToTest = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;
    
    dirtyFlag: () => DirtyFlag;
    
    constructor(dto: Raven.Client.Documents.Operations.ETL.RavenConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);
        
        this.update(dto);
        this.initValidation(); 
        
        const urlsCount = ko.pureComputed(() => this.topologyDiscoveryUrls().length);
        const urlsAreDirty = ko.pureComputed(() => {
            let anyDirty = false;
            
            this.topologyDiscoveryUrls().forEach(url => {
                if (url.dirtyFlag().isDirty()) {
                    anyDirty = true;
                }
            });
           
            return anyDirty;
        });
        
        this.dirtyFlag = new ko.DirtyFlag([           
            this.database,
            this.connectionStringName,
            urlsCount,
            urlsAreDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }    

    update(dto: Raven.Client.Documents.Operations.ETL.RavenConnectionString) {
        super.update(dto);
        
        this.connectionStringName(dto.Name); 
        this.database(dto.Database);
        this.topologyDiscoveryUrls(dto.TopologyDiscoveryUrls.map((x) => new discoveryUrl(x)));
    }

    initValidation() {
        super.initValidation();
        
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
        } as Raven.Client.Documents.Operations.ETL.RavenConnectionString, true, []);
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
        if ( !_.find(this.topologyDiscoveryUrls(), x => x.discoveryUrlName() === this.inputUrl().discoveryUrlName())) {
            const newUrl = new discoveryUrl(this.inputUrl().discoveryUrlName());
            newUrl.dirtyFlag().forceDirty();
            this.topologyDiscoveryUrls.unshift(newUrl);
            this.inputUrl().discoveryUrlName("");

            $(".collection-list li").first().addClass("blink-style");
        }
    }

    testConnection(urlToTest: string) : JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {       
        return new testClusterNodeConnectionCommand(urlToTest, this.database())
            .execute();
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand(db, this)
            .execute();
    }
}

export = connectionStringRavenEtlModel;
