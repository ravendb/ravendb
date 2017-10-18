/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");

class connectionStringRavenEtlModel extends connectionStringModel { 

    url = ko.observable<string>();                 
    database = ko.observable<string>();            

    validationGroup: KnockoutValidationGroup;
    testConnectionValidationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.ServerWide.ETL.RavenConnectionString, isNew: boolean, tasks: string[]) {
        super(dto, isNew, tasks);
        
        this.update(dto);       
        this.initValidation();      
    }    

    update(dto: Raven.Client.ServerWide.ETL.RavenConnectionString) {
        super.update(dto);
        
        this.connectionStringName(dto.Name); 
        this.database(dto.Database);
        this.url(dto.Url);
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

        this.url.extend({
            required: true,
            validUrl: true
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            database: this.database,
            url: this.url
        });
        
        this.testConnectionValidationGroup = ko.validatedObservable({
            url: this.url
        })
    }

    static empty(): connectionStringRavenEtlModel {
        return new connectionStringRavenEtlModel({
            Type: "Raven",
            Name: "", 
            Url: "",
            Database: ""
        } as Raven.Client.ServerWide.ETL.RavenConnectionString, true, []);
    }
    
    toDto() {
        return {
            Type: "Raven",
            Name: this.connectionStringName(),
            Url: this.url(),
            Database: this.database()
        };
    }
}

export = connectionStringRavenEtlModel;
