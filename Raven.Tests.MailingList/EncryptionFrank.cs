// -----------------------------------------------------------------------
//  <copyright file="EncryptionFrank.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Security.Cryptography;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Encryption.Settings;
using Raven.Bundles.Encryption.Streams;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.MailingList
{
    public class EncryptionFrank : NoDisposalNeeded
    {
        
        [Theory]
        [InlineData(10)]
        [InlineData(14300)]
        [InlineData(1024 * 2)]
        [InlineData(1024 * 4)]
        [InlineData(1024 * 8)]
        [InlineData(1024 * 16)]
        public void CryptoStream_should_read_and_write_properly(int expectedSizeInBytes)
        {
            var data = new byte[expectedSizeInBytes];
            new System.Random().NextBytes(data);

            const string encryptionKey = "Byax1jveejqio9Urcdjw8431iQYKkPg6Ig4OxHdxSAU=";
            var encryptionKeyBytes = Convert.FromBase64String(encryptionKey);
            var encryptionSettings = new EncryptionSettings(encryptionKeyBytes, typeof(RijndaelManaged), true, 128);

            var filename = Guid.NewGuid() + ".txt";
            try
            {

                using (var stream = new FileStream(filename, FileMode.CreateNew))
                using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
                {
                    cryptoStream.Write(data, 0, data.Length);
                    cryptoStream.Flush();
                }

                using (var stream = new FileStream(filename, FileMode.Open))
                using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
                {
                    var readData = cryptoStream.ReadData();
                    Assert.Equal(data.Length, readData.Length);
                    Assert.Equal(data, readData);
                }
            }
            finally
            {
                File.Delete(filename);
            }
        }


        [Theory]
        [InlineData(14300)]
        [InlineData(1024 * 2)]
        [InlineData(1024 * 4)]
        [InlineData(1024 * 8)]
        [InlineData(1024 * 16)]
        public void CryptoStream_should_show_unencrypted_length_properly(int expectedSizeInBytes)
        {
            var data = new byte[expectedSizeInBytes];
            new System.Random().NextBytes(data);

            const string encryptionKey = "Byax1jveejqio9Urcdjw8431iQYKkPg6Ig4OxHdxSAU=";
            var encryptionKeyBytes = Convert.FromBase64String(encryptionKey);
            var encryptionSettings = new EncryptionSettings(encryptionKeyBytes, typeof(RijndaelManaged), true, 128);

            var filename = Guid.NewGuid() + ".txt";
            try
            {

                using (var stream = new FileStream(filename, FileMode.CreateNew))
                using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
                {
                    cryptoStream.Write(data, 0, data.Length);
                    cryptoStream.Flush();
                }

                using (var stream = new FileStream(filename, FileMode.Open))
                using (var cryptoStream = new SeekableCryptoStream(encryptionSettings, encryptionKey, stream))
                {
                    Assert.Equal(data.Length, cryptoStream.Length);
                }
            }
            finally
            {
                File.Delete(filename);
            }
        } 
    }
}
