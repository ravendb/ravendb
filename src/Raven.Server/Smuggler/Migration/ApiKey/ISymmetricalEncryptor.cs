using System;
using System.Security.Cryptography;

namespace Raven.Server.Smuggler.Migration.ApiKey
{
    public interface ISymmetricalEncryptor : IDisposable
    {
        byte[] Key { get; set; }

        byte[] IV { get; set; }

        int KeySize { get; set; }

        void GenerateKey();

        void GenerateIV();

        ICryptoTransform CreateEncryptor();

        ICryptoTransform CreateDecryptor();

        ICryptoTransform CreateDecryptor(byte[] key, byte[] iv);
    }
}