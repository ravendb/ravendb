/// <reference path="../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import checkDomainAvailabilityCommand = require("commands/wizard/checkDomainAvailabilityCommand");

class domainInfo {
    private licenseProvider: () => Raven.Server.Commercial.License;
    
    domain = ko.observable<string>();     // i.e. "a chosen name" - Create new -or- Select from list
    rootDomain = ko.observable<string>(); // i.e. ".ravendb.run", ".development.run", etc - Select from dropdown only    
    userEmail = ko.observable<string>();  // Select from dropdown only
    
    availableDomains = ko.observableArray<string>([]);
    availableRootDomains = ko.observableArray<string>([]);
    availableEmails = ko.observableArray<string>([]);
    
    fullDomain: KnockoutComputed<string>;
    validationGroup: KnockoutValidationGroup;
    
    reusingConfiguration = ko.observable<boolean>(false);
    
    constructor(licenseProvider: () => Raven.Server.Commercial.License) {
        this.initValidation();
        this.licenseProvider = licenseProvider;
        
        this.fullDomain = ko.pureComputed(() => this.domain() + '.' + this.rootDomain());
    }
    
    private static tryExtractValidationError(result: JQueryXHR) {
        try {
            const json = JSON.parse(result.responseText);
            if (json && json["Error"]) {
                return json['Error'];
            }
        } catch (e) {
            // ignore
        }
        return null;
    }
    
    private initValidation() {

        const checkDomain = (val: string, 
                             params: any, 
                             callback: (currentValue: string, errorMessageOrValidationResult: boolean | string) => void) => {
                                                new checkDomainAvailabilityCommand(val, this.licenseProvider())
                                                    .execute()
                                                    .done((result: domainAvailabilityResult) => {
                                                        callback(this.domain(), result.Available || result.IsOwnedByMe); 
                                                    })
                                                    .fail((result: JQueryXHR) => {
                                                        if (result.status === 400) {
                                                            const error = domainInfo.tryExtractValidationError(result);
                                                            if (error) {
                                                                callback(this.domain(), error);
                                                            }
                                                        }
                                                    });
                             };
        
        this.domain.extend({
            required: true,
            pattern: {
                params: "^[a-zA-Z0-9-]+$",
                message: "Domain name can only contain A-Z, a-z, 0-9, '-'"
            },
            validation: {
                message: "Sorry, domain name is taken.",
                async: true,
                onlyIf: () => !!this.domain(),
                validator: generalUtils.debounceAndFunnel(checkDomain)
            }
        });

        this.rootDomain.extend({
            required: true            
        });
        
        this.userEmail.extend({
            required: true,
            email: true
        });
        
        this.validationGroup = ko.validatedObservable({
            domain: this.domain,
            rootDomain: this.rootDomain, 
            userEmail: this.userEmail
        });
    }
    
    setDomain(value: string) {
        this.domain(value);
    }
}

export = domainInfo;
