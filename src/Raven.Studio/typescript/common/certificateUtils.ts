/// <reference path="../../typings/tsd.d.ts" />

import forge = require("forge/forge");

class certificateUtils {
    public static readonly certificatePrefix = "-----BEGIN CERTIFICATE-----";
    public static readonly certificatePostfix = "-----END CERTIFICATE-----";
    
    static extractCertificateInfo(certificate: string): certificateInfo {
        if (!certificate.includes(certificateUtils.certificatePrefix)) {
            certificate = certificateUtils.certificatePrefix + "\r\n" + certificate + "\r\n" + certificateUtils.certificatePostfix;
        }

        const publicKeyParsed = forge.pki.certificateFromPem(certificate);

        return certificateUtils.extractCertificateInfoInternal(publicKeyParsed);
    }
    
    static extractCertificateFromPkcs12(certificate: string, password: string): string {
        const der = forge.util.decode64(certificate);
        const asn1 = forge.asn1.fromDer(der);
        const p12 = forge.pkcs12.pkcs12FromAsn1(asn1, password);
        
        const bags = p12.getBags({
            bagType: forge.pki.oids.certBag
        });

        const certBag = bags[forge.pki.oids.certBag][0];
        const cert = certBag.cert;
        
        return forge.pki.certificateToPem(cert);
    }
    
    private static extractCertificateInfoInternal(publicKeyParsed: forge.pki.Certificate): certificateInfo {
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
