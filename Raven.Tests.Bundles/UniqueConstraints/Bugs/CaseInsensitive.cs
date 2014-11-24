using Raven.Abstractions.Connection;
using Raven.Abstractions.Exceptions;
using Raven.Client.UniqueConstraints;

using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
    public class CaseInsensitive : UniqueConstraintsTest
    {
        public class Lizard
        {
            [UniqueConstraint(CaseInsensitive = true)]
            public string Name { get; set; }
        }

        [Fact]
        public void ShouldWork()
        {
            using (var session = DocumentStore.OpenSession())
            {
                session.Store(new Lizard { Name = "Joe Smith" });
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(new Lizard { Name = "Joe SMITH" });
                Assert.Throws<ErrorResponseException>(() => session.SaveChanges());
            }

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(new Lizard { Name = "Joe Smith" });
                Assert.Throws<ErrorResponseException>(() => session.SaveChanges());
            }
        }
    }
}