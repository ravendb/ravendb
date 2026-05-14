using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Basic
{
    public class AllowedPathsValidatorTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Replication)]
        public void Should_trim_whitespace_and_ignore_empty_allowed_paths()
        {
            using var validator = new AllowedPathsValidator(["   ", " issues/ABCD/* "]);
            using var context = JsonOperationContext.ShortTermSingleUse();

            using var matchingItem = new DocumentReplicationItem();
            matchingItem.Type = ReplicationBatchItem.ReplicationItemType.Document;
            matchingItem.Id = context.GetLazyString("issues/ABCD/1");

            using var nonMatchingItem = new DocumentReplicationItem();
            nonMatchingItem.Type = ReplicationBatchItem.ReplicationItemType.Document;
            nonMatchingItem.Id = context.GetLazyString("tasks/1");

            Assert.True(validator.ShouldAllow(matchingItem));
            Assert.False(validator.ShouldAllow(nonMatchingItem));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Should_keep_legacy_invalid_wildcard_patterns_operational_at_runtime()
        {
            using var validator = new AllowedPathsValidator([" issues* "]);
            using var context = JsonOperationContext.ShortTermSingleUse();

            using var matchingItem = new DocumentReplicationItem();
            matchingItem.Type = ReplicationBatchItem.ReplicationItemType.Document;
            matchingItem.Id = context.GetLazyString("issues/ABCD/1");

            using var nonMatchingItem = new DocumentReplicationItem();
            nonMatchingItem.Type = ReplicationBatchItem.ReplicationItemType.Document;
            nonMatchingItem.Id = context.GetLazyString("tasks/1");

            Assert.True(validator.ShouldAllow(matchingItem));
            Assert.False(validator.ShouldAllow(nonMatchingItem));
        }
    }
}
