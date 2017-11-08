/// <reference path="../../../typings/tsd.d.ts"/>

class certificateInfo {
    certificate = ko.observable<string>();
    certificatePassword = ko.observable<string>();
    certificateCNs = ko.observableArray<string>([]);

    validationGroup: KnockoutValidationGroup;

    constructor() {

        this.certificate.extend({
            required: true
        });

        this.certificate.extend({
            required: true
        });
        
        this.validationGroup = ko.validatedObservable({
            certificate: this.certificate,
            certificatePassword: this.certificatePassword
        });
    }
}

export = certificateInfo;
