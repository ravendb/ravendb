using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Sharding;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_24157 : RavenTestBase
    {
        public RavenDB_24157(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void CanGetRemoteConfigShardedShouldThrow(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var e = Assert.Throws<NotSupportedInShardingException>(() =>
                {
                    store.Maintenance.Send(new GetRemoteAttachmentsConfigurationOperation());
                });
                Assert.Contains("Remote attachments does not support sharding", e.Message);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void CanAddRemoteConfigShardedShouldThrow(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var id = "does-not-exist-identifier";
                var cfg = new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            id, new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings
                                {
                                    BucketName = "test-bucket-does-not-exist", AwsRegionName = "us-west-2", AwsAccessKey = "AKIAFAKEKEY", AwsSecretKey = "FAKESECRET"
                                },
                                Disabled = true,
                            }
                        }
                    },
                    CheckFrequencyInSec = 123
                };

                var e = Assert.Throws<NotSupportedInShardingException>(() =>
                {
                    store.Maintenance.Send(new ConfigureRemoteAttachmentsOperation(cfg));
                });
                Assert.Contains("Remote attachments does not support sharding", e.Message);
            }
        }
    }
}
