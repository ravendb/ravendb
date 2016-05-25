// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2181.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Database.Client.Aws;
using Raven.Database.Client.Azure;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2181 : NoDisposalNeeded
    {
        private const string AzureAccountName = "devstoreaccount1";
        private const string AzureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        [Fact(Skip = "Requires Windows Azure Development Storage")]
        public async Task PutBlob()
        {
            var containerName = "testContainer";
            var blobKey = "testKey";

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                client.PutContainer();
                await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>
                {
                    { "property1", "value1" }, 
                    { "property2", "value2" }
                });
                var blob = client.GetBlob(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal("123", reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
            }
        }

        [Fact(Skip = "Requires Windows Azure Development Storage")]
        public async Task PutBlobIntoFolder()
        {
            var containerName = "testContainer";
            var blobKey = "folder1/folder2/testKey";

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                client.PutContainer();
                await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>
                {
                    { "property1", "value1" }, 
                    { "property2", "value2" }
                });
                var blob = client.GetBlob(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal("123", reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
            }
        }

        [Fact(Skip = "Requires Amazon AWS Credentials")]
        public void PutObject()
        {
            var bucketName = "ravendb";
            var key = "testKey";

            using (var client = new RavenAwsS3Client("<aws_access_key>", "<aws_secret_key>", "<aws_region_for_bucket>"))
            {
                client.PutObject(bucketName, key, new MemoryStream(Encoding.UTF8.GetBytes("321")), new Dictionary<string, string>
                                                                                                        {
                                                                                                            { "property1", "value1" }, 
                                                                                                            { "property2", "value2" }
                                                                                                        }, 60 * 60);
                var @object = client.GetObject(bucketName, key);
                Assert.NotNull(@object);

                using (var reader = new StreamReader(@object.Data))
                    Assert.Equal("321", reader.ReadToEnd());

                var property1 = @object.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = @object.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal("value1", @object.Metadata[property1]);
                Assert.Equal("value2", @object.Metadata[property2]);
            }
        }

        [Fact(Skip = "Requires Amazon AWS Credentials")]
        public void UploadArchive()
        {
            var glacierVaultName = "ravendb";

            using (var client = new RavenAwsGlacierClient("<aws_access_key>", "<aws_secret_key>", "<aws_region_for_bucket>"))
            {
                var archiveId = client.UploadArchive(glacierVaultName, new MemoryStream(Encoding.UTF8.GetBytes("321")), "sample description", 60 * 60);

                Assert.NotNull(archiveId);
            }
        }

        [Fact]
        public void AuthorizationHeaderValueForAwsS3ShouldBeCalculatedCorrectly1()
        {
            using (var client = new RavenAwsS3Client("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1"))
            {
                var date = new DateTime(2013, 5, 24);

                var stream = new MemoryStream(Encoding.UTF8.GetBytes("Welcome to Amazon S3."));
                var payloadHash = RavenAwsHelper.CalculatePayloadHash(stream);

                Assert.Equal("44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072", payloadHash);

                var url = client.GetUrl("examplebucket") + "/" + "test%24file.text";
                var headers = new Dictionary<string, string>
                              {
                                  { "x-amz-date", RavenAwsHelper.ConvertToString(date) }, 
                                  { "x-amz-content-sha256", payloadHash }, 
                                  { "x-amz-storage-class", "REDUCED_REDUNDANCY" },
                                  { "Date", date.ToString("R") },
                                  { "Host", "examplebucket.s3.amazonaws.com" }
                              };

                var auth = client.CalculateAuthorizationHeaderValue("PUT", url, date, headers);

                Assert.Equal("AWS4-HMAC-SHA256", auth.Scheme);
                Assert.Equal("Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd", auth.Parameter);
            }
        }

        [Fact]
        public void AuthorizationHeaderValueForAwsS3ShouldBeCalculatedCorrectly2()
        {
            using (var client = new RavenAwsS3Client("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1"))
            {
                var date = new DateTime(2013, 5, 24);
                var payloadHash = RavenAwsHelper.CalculatePayloadHash(null);

                Assert.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", payloadHash);

                var url = client.GetUrl("examplebucket") + "/" + "test.txt";
                var headers = new Dictionary<string, string>
                              {
                                  { "x-amz-date", RavenAwsHelper.ConvertToString(date) }, 
                                  { "x-amz-content-sha256", payloadHash }, 
                                  { "Date", date.ToString("R") },
                                  { "Host", "examplebucket.s3.amazonaws.com" },
                                  { "Range", "bytes=0-9"}
                              };

                var auth = client.CalculateAuthorizationHeaderValue("GET", url, date, headers);

                Assert.Equal("AWS4-HMAC-SHA256", auth.Scheme);
                Assert.Equal("Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", auth.Parameter);
            }
        }
    }
}
