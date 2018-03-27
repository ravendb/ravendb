using System;
using System.Security.Cryptography;

namespace Raven.Server.Smuggler.Migration.ApiKey
{
    public interface IAsymmetricalEncryptor : IDisposable
    {
        int KeySize { get; set; }

        void ImportParameters(byte[] exponent, byte[] modulus);

        byte[] Encrypt(byte[] bytes, bool fOAEP);

        void ImportCspBlob(byte[] keyBlob);

        byte[] ExportCspBlob(bool includePrivateParameters);

        byte[] SignHash(byte[] hash, string str);

        bool VerifyHash(byte[] hash, string str, byte[] signature);

        byte[] Decrypt(byte[] bytes, bool fOAEP);

        AsymmetricAlgorithm Algorithm { get; }

        void ImportParameters(RSAParameters parameters);

        RSAParameters ExportParameters(bool includePrivateParameters);
    }
}
