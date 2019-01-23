/// <reference path="../../../../typings/tsd.d.ts"/>

import certificateUtils = require("common/certificateUtils");

class pullReplicationCertificate {
    
    publicKey = ko.observable<string>();
    thumbprint = ko.observable<string>();
    expirationText = ko.observable<string>();
    expirationIcon = ko.observable<string>();
    expirationClass = ko.observable<string>();
    validFromText = ko.observable<string>();

    certificate = ko.observable<string>();
    certificatePassphrase = ko.observable<string>();
    
    constructor(publicKey: string, certificate: string = undefined, password: string = undefined) {
        this.publicKey(publicKey);
        this.certificate(certificate);
        this.certificatePassphrase(password);
        
        const certInfo = certificateUtils.extractCertificateInfo(publicKey);
        this.thumbprint(certInfo.thumbprint);

        const expirationMoment = moment.utc(certInfo.expiration);
        const dateFormatted = expirationMoment.format("YYYY-MM-DD");
        
        if (expirationMoment.isBefore()) {
            this.expirationText("Expired " + dateFormatted);
            this.expirationIcon("icon-danger");
            this.expirationClass("text-danger");
        } else  {
            this.expirationText(dateFormatted);
            this.expirationIcon("icon-clock");
            this.expirationClass("");
        }
        
        const notBeforeMoment = moment.utc(certInfo.notBefore);
        
        this.validFromText(notBeforeMoment.format("YYYY-MM-DD"));
    }
    
    static fromPublicKey(certificate: string) {
        return new pullReplicationCertificate(certificate, null);
    }
    
    static fromPkcs12(certificate: string, password: string = undefined) {
        const publicKey = certificateUtils.extractCertificateFromPkcs12(certificate, password);
        return new pullReplicationCertificate(publicKey, certificate, password);
    }
    
}

export = pullReplicationCertificate;
