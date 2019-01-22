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
    
    constructor(publicKey: string, certificate: string = undefined) {
        this.publicKey(publicKey);
        this.certificate(certificate);
        
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
    
    static tryParse(cert: string) {
        if (!cert.includes("----")) {
            // looks like --- BEGIN CERTIFICATE IS MISSING try to append this
            cert = certificateUtils.certificatePrefix + "\r\n" + cert + "\r\n" + certificateUtils.certificatePostfix;
        }
        
        return new pullReplicationCertificate(cert);
    }
    
}

export = pullReplicationCertificate;
