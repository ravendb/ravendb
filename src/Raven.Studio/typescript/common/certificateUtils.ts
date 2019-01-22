/// <reference path="../../typings/tsd.d.ts" />

import forge = require("node-forge");

class certificateUtils {
    public static readonly certificatePrefix = "-----BEGIN CERTIFICATE-----";
    public static readonly certificatePostfix = "-----END CERTIFICATE-----";
    
    static extractCertificateInfo(certificate: string): certificateInfo {
        const certWithHeaders = certificateUtils.certificatePrefix + "\r\n" + certificate + "\r\n" + certificateUtils.certificatePostfix;

        const publicKeyParsed = forge.pki.certificateFromPem(certWithHeaders);
        const certAsn1 = forge.pki.certificateToAsn1(publicKeyParsed);
        const derEncodedCert = forge.asn1.toDer(certAsn1).getBytes();

        const thumbprint = forge.md.sha1.create().update(derEncodedCert).digest().toHex().toLocaleUpperCase();
        
        const expiration = publicKeyParsed.validity.notAfter; 
        const notBefore = publicKeyParsed.validity.notBefore;
        return {
            thumbprint,
            expiration,
            notBefore
        }
    }
} 

export = certificateUtils;
