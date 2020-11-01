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
    
    static extractCertificatesFromPkcs12(base64EncodedCertificate: string, password: string): string[] {
        const der = forge.util.decode64(base64EncodedCertificate);
        const asn1 = forge.asn1.fromDer(der);
        const p12 = forge.pkcs12.pkcs12FromAsn1(asn1, password);
        
        const bags = p12.getBags({
            bagType: forge.pki.oids.certBag
        });
        
        return bags[forge.pki.oids.certBag].map(x => forge.pki.certificateToPem(x.cert));
    }
    
    static extractCertificateFromPkcs12(base64EncodedCertificate: string, password: string): string {
        const pems = certificateUtils.extractCertificatesFromPkcs12(base64EncodedCertificate, password);
        
        if (pems.length > 1) {
            throw new Error("File contains multiple certificates. Please extract and upload a single certificate.");
        }
        
        return pems.length ? pems[0] : null;
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
    
    static extractBase64(publicKey: string) {
        // extract the base 64 from a pem format
        let base64 = publicKey.replace(certificateUtils.certificatePrefix, "");
        base64 = base64.replace(certificateUtils.certificatePostfix, "");
        base64 = base64.replace(/(\r\n|\n|\r)/g, "");
        return base64.trim();
    }
} 

export = certificateUtils;
