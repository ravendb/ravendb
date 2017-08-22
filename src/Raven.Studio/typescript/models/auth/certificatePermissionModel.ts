/// <reference path="../../../typings/tsd.d.ts" />


class certificatePermissionModel {
    databaseName = ko.observable<string>();
    accessLevel = ko.observable<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess>("ReadWrite");
    
    validationGroup = ko.validatedObservable({
        databaseName: this.databaseName,
        accessLevel: this.accessLevel
    });
    
    constructor() {
        this.initValidation();
    }
    
    private initValidation() {
        this.databaseName.extend({
            required: true
        });
        
        this.accessLevel.extend({
            required: true
        });
    }
    
}

export = certificatePermissionModel;
