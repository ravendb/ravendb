/// <reference path="../../../typings/tsd.d.ts"/>

class certificateInfo {
    certificateFileName = ko.observable<string>();
    fileProtected = ko.observable<boolean>(false);
    
    certificate = ko.observable<string>();
    certificatePassword = ko.observable<string>();
    certificateCNs = ko.observableArray<string>([]);

    wildcardCertificate: KnockoutComputed<boolean>;
    expirationDateFormatted: KnockoutComputed<string>;
    
    validationGroup: KnockoutValidationGroup;

    constructor() {
        this.initValidation();
        this.initObservables();
    }
    
    private initObservables() {
        this.wildcardCertificate = ko.pureComputed(() => {
            const cns = this.certificateCNs();
            return cns.some(x => x.startsWith("*"));
            
            // Currently, for the Setup Wizard flow, the server supports only the following 2 cases:
            // 1. pfx file with Single Wildcard value (*.someDomain)
            // 2. pfx file with Single -or- Multiple values of Non-Wildcard values (localhost, localhost2...)
            // For all other cases, the server should throw an exception (Also, the two types should not be mixed)
        });
    }

    private initValidation() {
        this.certificate.extend({
            required: true
        });

        this.certificateCNs.extend({
            validation: [{
                validator: (val: Array<string>) => val.length > 0,
                message: `Certificate must contain at least one CN or Subject Alternative Name.`
            }]
        });

        this.validationGroup = ko.validatedObservable({
            certificate: this.certificate,
            certificatePassword: this.certificatePassword,
            certificateCNs: this.certificateCNs
        });
    }
}

export = certificateInfo;
