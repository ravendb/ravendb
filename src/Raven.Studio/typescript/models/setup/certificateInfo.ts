/// <reference path="../../../typings/tsd.d.ts"/>

class certificateInfo {
    certificate = ko.observable<string>();
    certificatePassword = ko.observable<string>();
    certificateCNs = ko.observableArray<string>([]);

    wildcardCertificate: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup;

    constructor() {
        this.initValidation();
        
        this.wildcardCertificate = ko.pureComputed(() => {
            const cns = this.certificateCNs();
            return _.some(cns, x => x.startsWith("*"));
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
