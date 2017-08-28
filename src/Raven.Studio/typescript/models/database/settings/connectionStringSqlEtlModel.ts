/// <reference path="../../../../typings/tsd.d.ts"/>

class connectionStringSqlEtlModel {

    connectionStringName = ko.observable<string>(); 
    connectionString = ko.observable<string>();     
    
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.ServerWide.ETL.SqlConnectionString) {
        this.update(dto);
        this.initValidation();
    }

    update(dto: Raven.Client.ServerWide.ETL.SqlConnectionString) {
        this.connectionStringName(dto.Name); 
        this.connectionString(dto.ConnectionString); 
    }

    initValidation() {
        this.connectionStringName.extend({
            required: true
        });

        this.connectionString.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            connectionString: this.connectionString
        });
    }

    static empty(): connectionStringSqlEtlModel {
        return new connectionStringSqlEtlModel({
            Type: "Sql",
            Name: "",
            ConnectionString: ""
        } as Raven.Client.ServerWide.ETL.SqlConnectionString);
    }
    
    toDto() {
        return {
            Type: "Sql",
            Name: this.connectionStringName(),
            ConnectionString: this.connectionString()
        };
    }
}

export = connectionStringSqlEtlModel;
