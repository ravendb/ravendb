using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Sharding;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24157 : RavenTestBase
    {
        public RavenDB_24157(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void CanGetRetireConfigShardedShouldThrow(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var e = Assert.Throws<NotSupportedInShardingException>(() =>
                {
                    store.Maintenance.Send(new GetRetireAttachmentsConfigurationOperation());
                });
                Assert.Contains("Retired attachments does not support sharding", e.Message);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void CanAddRetireConfigShardedShouldThrow(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var id = "does-not-exist-identifier";
                var cfg = new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            id, new RetiredAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new S3Settings
                                {
                                    BucketName = "test-bucket-does-not-exist", AwsRegionName = "us-west-2", AwsAccessKey = "AKIAFAKEKEY", AwsSecretKey = "FAKESECRET"
                                },
                                Disabled = true,
                                Identifier = id
                            }
                        }
                    },
                    RetireFrequencyInSec = 123
                };

                var e = Assert.Throws<NotSupportedInShardingException>(() =>
                {
                    store.Maintenance.Send(new ConfigureRetiredAttachmentsOperation(cfg));
                });
                Assert.Contains("Retired attachments does not support sharding", e.Message);
            }
        }
    }
}
