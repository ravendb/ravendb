/// <reference path="../../../../typings/tsd.d.ts"/>
import certificateUtils = require("common/certificateUtils");
import moment = require("moment");

class replicationCertificateModel {

    certificate = ko.observable<string>(); // public & private key
    certificatePassphrase = ko.observable<string>();
    
    publicKey = ko.observable<string>();
    thumbprint = ko.observable<string>();
    
    expirationText = ko.observable<string>();
    expirationIcon = ko.observable<string>();
    expirationClass = ko.observable<string>();
    
    validFromText = ko.observable<string>();
    
    constructor(publicKey: string, base64EncodedCertificate: string = undefined, password: string = undefined) {
        
        this.publicKey(certificateUtils.extractBase64(publicKey));
       
        if (base64EncodedCertificate) {
            this.certificate(base64EncodedCertificate);
        }
        
        this.certificatePassphrase(password);
        
        const certInfo = certificateUtils.extractCertificateInfo(publicKey);
        this.thumbprint(certInfo.thumbprint);

        const expirationMoment = moment.utc(certInfo.expiration);
        const dateFormatted = expirationMoment.format("YYYY-MM-DD");
        
        if (expirationMoment.isBefore()) {
            this.expirationText("Expired " + dateFormatted);
            this.expirationIcon("icon-danger");
            this.expirationClass("text-danger");
        } else {
            this.expirationText(dateFormatted);
            this.expirationIcon("icon-expiration");
            this.expirationClass("");
        }
        
        const notBeforeMoment = moment.utc(certInfo.notBefore);
        
        this.validFromText(notBeforeMoment.format("YYYY-MM-DD"));
    }
    
    static fromPublicKey(certificate: string) {
        return new replicationCertificateModel(certificate, null);
    }
    
    static fromPkcs12(base64EncodedCertificate: string, password: string = undefined) {
        const publicKey = certificateUtils.extractCertificateFromPkcs12(base64EncodedCertificate, password);
        return new replicationCertificateModel(publicKey, base64EncodedCertificate, password);
    }
}

export = replicationCertificateModel;
