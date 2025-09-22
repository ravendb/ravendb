using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Sparrow;

namespace Raven.Client.Util;

internal static class PublicKeyPinningHashHelpers
{
    public static string GetPublicKeyPinningHash(this X509Certificate2 cert)
    {
        //Get the SubjectPublicKeyInfo member of the certificate
        var subjectPublicKeyInfo = GetSubjectPublicKeyInfoRaw(cert);

        //Take the SHA2-256 hash of the DER ASN.1 encoded value
        byte[] digest;
        using (var sha2 = SHA256.Create())
        {
            digest = sha2.ComputeHash(subjectPublicKeyInfo);
        }

        //Convert hash to base64
        var hash = Convert.ToBase64String(digest);

        return hash;
    }

    public static unsafe byte[] GetSubjectPublicKeyInfoRaw(X509Certificate2 cert)
    {
        /*
             Certificate is, by definition:

                Certificate  ::=  SEQUENCE  {
                    tbsCertificate       TBSCertificate,
                    signatureAlgorithm   AlgorithmIdentifier,
                    signatureValue       BIT STRING
                }

               TBSCertificate  ::=  SEQUENCE  {
                    version         [0]  EXPLICIT Version DEFAULT v1,
                    serialNumber         CertificateSerialNumber,
                    signature            AlgorithmIdentifier,
                    issuer               Name,
                    validity             Validity,
                    subject              Name,
                    subjectPublicKeyInfo SubjectPublicKeyInfo,
                    issuerUniqueID  [1]  IMPLICIT UniqueIdentifier OPTIONAL, -- If present, version MUST be v2 or v3
                    subjectUniqueID [2]  IMPLICIT UniqueIdentifier OPTIONAL, -- If present, version MUST be v2 or v3
                    extensions      [3]  EXPLICIT Extensions       OPTIONAL  -- If present, version MUST be v3
                }

            So we walk the ASN.1 DER tree in order to drill down to the SubjectPublicKeyInfo item
            */

        var rawCert = cert.GetRawCertData();
        var bufferLength = rawCert.Length;

        fixed (byte* certPtr = rawCert)
        {
            var ptr = AsnNext(certPtr, ref bufferLength, true, false);  // unwrap certificate sequence
            ptr = AsnNext(ptr, ref bufferLength, false, false); // get tbsCertificate
            ptr = AsnNext(ptr, ref bufferLength, true, false);  // unwrap tbsCertificate sequence
            ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Version
            ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.SerialNumber
            ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Signature
            ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Issuer
            ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Validity
            ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Subject
            ptr = AsnNext(ptr, ref bufferLength, false, false); // get tbsCertificate.SubjectPublicKeyInfo

            var subjectPublicKeyInfo = new byte[bufferLength];
            fixed (byte* newPtr = subjectPublicKeyInfo)
            {
                Memory.Copy(newPtr, ptr, bufferLength);
            }
            return subjectPublicKeyInfo;
        }
    }

    private static unsafe byte* AsnNext(byte* buffer, ref int bufferLength, bool unwrap, bool getRemaining)
    {
        if (bufferLength < 2)
        {
            return buffer;
        }

        var index = 0;
        index++;

        int length = buffer[index];
        index++;

        var lengthBytes = 1;
        if (length >= 0x80)
        {
            lengthBytes = length & 0x0F; //low nibble is number of length bytes to follow
            length = 0;

            for (var i = 0; i < lengthBytes; i++)
            {
                length = (length << 8) + (int)buffer[index + i];
            }
            lengthBytes++;
        }

        int skip;
        int take;
        if (unwrap)
        {
            skip = 1 + lengthBytes;
            take = length;
        }
        else
        {
            skip = 0;
            take = 1 + lengthBytes + length;
        }

        if (getRemaining == false)
        {
            buffer += skip;
            bufferLength = take;
        }
        else
        {
            buffer += skip + take;
            bufferLength -= (skip + take);
        }

        return buffer;
    }
}
