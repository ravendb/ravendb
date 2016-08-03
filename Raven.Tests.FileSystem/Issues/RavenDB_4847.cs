// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4847.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.FileSystem.Storage;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4847 : RavenFilesTestBase
    {
        private const string FileName = "encrypted.bin";

        [Theory]
        [InlineData("voron", 100)]
        [InlineData("esent", 100)]
        [InlineData("voron", 1000)]
        [InlineData("esent", 1000)]
        [InlineData("voron", StorageConstants.MaxPageSize)]
        [InlineData("esent", StorageConstants.MaxPageSize)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2 + 1)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2 + 1)]
        public async Task can_upload_and_calculate_md5_stream(string storage, int stringSize)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                var client = store.AsyncFilesCommands;
                var str = RandomString(stringSize);

                byte[] originalHash;
                using (var stream = new MemoryStream())
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(str);
                    writer.Flush();
                    stream.Position = 0;

                    using (var md5Hash = MD5.Create())
                    using (var cryptoStream = new CryptoStream(stream, md5Hash, CryptoStreamMode.Read))
                    {
                        await client.UploadAsync(FileName, cryptoStream).ConfigureAwait(false);

                        originalHash = md5Hash.Hash;
                    }
                }

                using (var md5Hash = MD5.Create())
                using (var downloadStream = await client.DownloadAsync(FileName))
                using (var cryptoStream = new CryptoStream(downloadStream, md5Hash, CryptoStreamMode.Read))
                using (var streamReader = new StreamReader(cryptoStream))
                {
                    var data = streamReader.ReadToEnd();
                    Assert.Equal(str, data);
                    Assert.Equal(originalHash, md5Hash.Hash);
                }
            }
        }

        [Theory]
        [InlineData("voron", 100)]
        [InlineData("esent", 100)]
        [InlineData("voron", 1000)]
        [InlineData("esent", 1000)]
        [InlineData("voron", StorageConstants.MaxPageSize)]
        [InlineData("esent", StorageConstants.MaxPageSize)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2 + 1)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2 + 1)]
        public async Task can_upload_and_calculate_md5_stream_overload1(string storage, int stringSize)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                var client = store.AsyncFilesCommands;
                var str = RandomString(stringSize);

                byte[] originalHash;
                using (var stream = new MemoryStream())
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(str);
                    writer.Flush();
                    stream.Position = 0;

                    using (var md5Hash = MD5.Create())
                    using (var cryptoStream = new CryptoStream(stream, md5Hash, CryptoStreamMode.Read))
                    {
                        var metadata = new RavenJObject { [Constants.CreationDate] = new DateTimeOffset() };
                        await client.UploadRawAsync(FileName, cryptoStream, metadata, null).ConfigureAwait(false);

                        originalHash = md5Hash.Hash;
                    }
                }

                using (var md5Hash = MD5.Create())
                using (var downloadStream = await client.DownloadAsync(FileName))
                using (var cryptoStream = new CryptoStream(downloadStream, md5Hash, CryptoStreamMode.Read))
                using (var streamReader = new StreamReader(cryptoStream))
                {
                    var data = streamReader.ReadToEnd();
                    Assert.Equal(str, data);
                    Assert.Equal(originalHash, md5Hash.Hash);
                }
            }
        }

        [Theory]
        [InlineData("voron", 100)]
        [InlineData("esent", 100)]
        [InlineData("voron", 1000)]
        [InlineData("esent", 1000)]
        [InlineData("voron", StorageConstants.MaxPageSize)]
        [InlineData("esent", StorageConstants.MaxPageSize)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2 + 1)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2 + 1)]
        public async Task can_upload_and_calculate_md5_stream_overload2(string storage, int stringSize)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                var client = store.AsyncFilesCommands;
                var str = RandomString(stringSize);

                byte[] originalHash = null;
                await client.UploadAsync(FileName, s =>
                {
                    using (var md5Hash = MD5.Create())
                    {
                        using (var cryptoStream = new CryptoStream(s, md5Hash, CryptoStreamMode.Write))
                        using (var writer = new StreamWriter(cryptoStream))
                        {
                            writer.Write(str);
                        }
                        originalHash = md5Hash.Hash;
                    }
                }, null, null).ConfigureAwait(false);

                using (var md5Hash = MD5.Create())
                using (var downloadStream = await client.DownloadAsync(FileName))
                using (var cryptoStream = new CryptoStream(downloadStream, md5Hash, CryptoStreamMode.Read))
                using (var streamReader = new StreamReader(cryptoStream))
                {
                    var data = streamReader.ReadToEnd();
                    Assert.Equal(str, data);
                    Assert.Equal(originalHash, md5Hash.Hash);
                }
            }
        }

        [Theory]
        [InlineData("voron", 100)]
        [InlineData("esent", 100)]
        [InlineData("voron", 1000)]
        [InlineData("esent", 1000)]
        [InlineData("voron", StorageConstants.MaxPageSize)]
        [InlineData("esent", StorageConstants.MaxPageSize)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2 + 1)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2 + 1)]
        public async Task can_upload_encrypted_stream(string storage, int stringSize)
        {
            var cryptic = new DESCryptoServiceProvider();

            using (var store = NewStore(requestedStorage: storage))
            {
                var client = store.AsyncFilesCommands;
                var str = RandomString(stringSize);

                await client.UploadAsync(FileName, s =>
                {
                    using (var cryptoStream = new CryptoStream(s, cryptic.CreateEncryptor(), CryptoStreamMode.Write))
                    using (var writer = new StreamWriter(cryptoStream))
                    {
                        writer.Write(str);
                    }
                }, null, null).ConfigureAwait(false);

                using (var downloadStream = await client.DownloadAsync(FileName))
                using (var cryptoStream = new CryptoStream(downloadStream, cryptic.CreateDecryptor(), CryptoStreamMode.Read))
                using (var streamReader = new StreamReader(cryptoStream))
                {
                    var data = streamReader.ReadToEnd();
                    Assert.Equal(str, data);
                }
            }
        }

        [Theory]
        [InlineData("voron", 100, CompressionMode.Compress)]
        [InlineData("voron", 100, CompressionLevel.Optimal)]
        [InlineData("esent", 100, CompressionMode.Compress)]
        [InlineData("esent", 100, CompressionLevel.Optimal)]
        [InlineData("voron", 1000, CompressionMode.Compress)]
        [InlineData("voron", 1000, CompressionLevel.Optimal)]
        [InlineData("esent", 1000, CompressionMode.Compress)]
        [InlineData("esent", 1000, CompressionLevel.Optimal)]
        [InlineData("voron", StorageConstants.MaxPageSize, CompressionMode.Compress)]
        [InlineData("voron", StorageConstants.MaxPageSize, CompressionLevel.Optimal)]
        [InlineData("esent", StorageConstants.MaxPageSize, CompressionMode.Compress)]
        [InlineData("esent", StorageConstants.MaxPageSize, CompressionLevel.Optimal)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2, CompressionMode.Compress)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2, CompressionLevel.Optimal)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2, CompressionMode.Compress)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2, CompressionLevel.Optimal)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2 + 1, CompressionMode.Compress)]
        [InlineData("voron", StorageConstants.MaxPageSize * 2 + 1, CompressionLevel.Optimal)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2 + 1, CompressionMode.Compress)]
        [InlineData("esent", StorageConstants.MaxPageSize * 2 + 1, CompressionLevel.Optimal)]
        public async Task can_upload_gzip_stream(string storage, int stringSize, CompressionLevel compressionLevel)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                var str = RandomString(stringSize);

                var client = store.AsyncFilesCommands;
                await client.UploadAsync(FileName, s =>
                {
                    using (var gzipStream = new GZipStream(s, compressionLevel))
                    using (var writer = new StreamWriter(gzipStream))
                    {
                        writer.Write(str);
                    }
                }, null, null).ConfigureAwait(false);

                using (var downloadStream = await client.DownloadAsync(FileName))
                using (var gzipStream = new GZipStream(downloadStream, CompressionMode.Decompress))
                using (var streamReader = new StreamReader(gzipStream))
                {
                    var data = streamReader.ReadToEnd();
                    Assert.Equal(str, data);
                }
            }
        }

        private static readonly Random Random = new Random();
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[Random.Next(s.Length)]).ToArray());
        }
    }
}
