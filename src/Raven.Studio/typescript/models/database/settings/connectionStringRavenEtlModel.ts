/// <reference path="../../../../typings/tsd.d.ts"/>

class connectionStringRavenEtlModel { 

    connectionStringName = ko.observable<string>(); 
    url = ko.observable<string>();                 
    database = ko.observable<string>();            

    validationGroup: KnockoutValidationGroup;
    testConnectionValidationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.ServerWide.ETL.RavenConnectionString) {
        this.update(dto);
        this.initValidation();
    }    

    update(dto: Raven.Client.ServerWide.ETL.RavenConnectionString) {
        this.connectionStringName(dto.Name); 
        this.database(dto.Database);
        this.url(dto.Url);
    }

    initValidation() {
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
        } as Raven.Client.ServerWide.ETL.RavenConnectionString);
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
