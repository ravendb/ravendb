using Raven.Client;

using System;

using Xunit;

using Raven.Client.UniqueConstraints;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
    public class RavenDB_1499 : UniqueConstraintsTest
    {
        [Fact]
        public void LoadByUniqueConstraint_should_throw_a_meaningful_exception_if_the_attribute_isnt_found()
        {
            using (var session = DocumentStore.OpenSession())
            {
                Assert.Throws<InvalidOperationException>(() => DoLoad<WikiPage>(session));
            }
        }

        static void DoLoad<T>(IDocumentSession session) where T : IHaveSlug
        {
            var loaded = session.LoadByUniqueConstraint<T>(cat => cat.Slug, Guid.NewGuid());
        }
    }

    public interface IHaveSlug
    {
        string Slug { get; set; }
    }

    public class WikiPage : IHaveSlug
    {
        [UniqueConstraint]
        public string Slug { get; set; }

        public string Body { get; set; }
    }
}