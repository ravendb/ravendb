/// <reference path="../../../typings/tsd.d.ts"/>

class licenseInfo {
    license = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        this.initValidation();
    }
    
    private initValidation() {
        this.license.extend({
            required: true,
            validLicense: true
        });
        
        this.validationGroup = ko.validatedObservable({
            license: this.license
        });
    }
    
    toDto(): Raven.Server.Commercial.License {
        return JSON.parse(this.license()) as Raven.Server.Commercial.License;
    }
}

export = licenseInfo;
