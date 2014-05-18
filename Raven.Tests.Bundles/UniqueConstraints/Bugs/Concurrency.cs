using System;

using Raven.Client.UniqueConstraints;

using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
    public class TestWithOptimisticConcurrency : UniqueConstraintsTest
    {
        public class WithUniqueId
        {
            [UniqueConstraint]
            public string UniqueId { get; set; }
            public DateTime Updated { get; set; }
        }

        [Fact]
        public void CanUpdate()
        {
            var uuid = Guid.NewGuid().ToString("N");
            using (var s = DocumentStore.OpenSession())
            {
                s.Store(new WithUniqueId { UniqueId = uuid });
                s.SaveChanges();
            }

            using (var s = DocumentStore.OpenSession())
            {
                s.Advanced.UseOptimisticConcurrency = true;

                var doc = s.LoadByUniqueConstraint<WithUniqueId>(x => x.UniqueId, uuid);
                doc.Updated = DateTime.UtcNow;
                s.SaveChanges();
            }
        }
    }
}