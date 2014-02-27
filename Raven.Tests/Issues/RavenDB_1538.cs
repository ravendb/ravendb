using System;

using Raven.Client.UniqueConstraints;
using Raven.Json.Linq;
using Raven.Tests.Bundles.UniqueConstraints;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1538 : UniqueConstraintsTest
    {
        public class Doc
        {
            public string Id { get; set; }

            [UniqueConstraint(CaseInsensitive = false)]
            public string AlternateIdentifier { get; set; }
        }

        [Fact]
        public void Case_sensitive_constraint_should_work()
        {
            // Since the constraint is supposed to be case-sensitive, these should both be allowed.
            using (var session = DocumentStore.OpenSession())
            {
                var sample1 = new Doc { AlternateIdentifier = "aaa" };
                session.Store(sample1);
                session.SaveChanges();
            }
            // The following should be allowed to be stored
            using (var session = DocumentStore.OpenSession())
            {
                var sample2 = new Doc { AlternateIdentifier = "aaA" };
                session.Store(sample2);
                session.SaveChanges();
            }
        }

        [Fact]
        public void Case_sensitive_constraint_should_work_even_when_encoding_is_needed()
        {
            // because Base64 encoding is case-sensitive, the encodings of these two distinct identifiers collide when they're stored in the case-insensitive document key
            // aaa# is encoded as YWFhIw==
            // aaG# is encoded as YWFHIw==
            // There are an unbounded number of similar examples.
            using (var session = DocumentStore.OpenSession())
            {
                var sample1 = new Doc { AlternateIdentifier = "aaa#" };
                session.Store(sample1);
                session.SaveChanges();
            }
            // The following should be allowed to be stored
            using (var session = DocumentStore.OpenSession())
            {
                var sample2 = new Doc { AlternateIdentifier = "aaG#" };
                session.Store(sample2);
                session.SaveChanges();
            }
        }

        [Fact]
        public void Client_should_be_able_to_work_on_old_unique_constraint_document()
        {
            DocumentStore.DatabaseCommands.Put("docs/10", null, RavenJObject.FromObject(new { AlternateIdentifier = "aaa" }), new RavenJObject());
            DocumentStore.DatabaseCommands.Put("UniqueConstraints/Docs/AlternateIdentifier/aaa", null, RavenJObject.FromObject(new { RelatedId = "docs/10" }), new RavenJObject()); // old format

            using (var session = DocumentStore.OpenSession())
            {
                var sample1 = new Doc { AlternateIdentifier = "aaa" };
                Assert.False(session.CheckForUniqueConstraints(sample1).ConstraintsAreFree());
            }
        }

        [Fact]
        public void Server_should_convert_old_unique_constraint_document()
        {
            var key = "UniqueConstraints/Docs/AlternateIdentifier/aaa";
            DocumentStore.DatabaseCommands.Put(key, null, RavenJObject.FromObject(new { RelatedId = "docs/1" }), new RavenJObject()); // old format

            using (var session = DocumentStore.OpenSession())
            {
                var doc = session.Load<UniqueConstraintExtensions.ConstraintDocument>(key);
                
                Assert.NotNull(doc);
                Assert.Equal(0, doc.Constraints.Count);
                Assert.Equal("docs/1", doc.RelatedId);

                var sample1 = new Doc { AlternateIdentifier = "aaa" };
                session.Store(sample1);
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var doc = session.Load<UniqueConstraintExtensions.ConstraintDocument>(key);

                Assert.NotNull(doc);
                Assert.Equal(1, doc.Constraints.Count);
                Assert.Equal("docs/1", doc.Constraints["aaa"].RelatedId);
                Assert.Null(doc.RelatedId);
            }
        }

        [Fact]
        public void Removing_values_from_unique_constraint_document_should_work()
        {
            string docId1, docId2;

            // Since the constraint is supposed to be case-sensitive, these should both be allowed.
            using (var session = DocumentStore.OpenSession())
            {
                var sample1 = new Doc { AlternateIdentifier = "aaa" };
                session.Store(sample1);
                session.SaveChanges();

                docId1 = sample1.Id;
            }
            // The following should be allowed to be stored
            using (var session = DocumentStore.OpenSession())
            {
                var sample2 = new Doc { AlternateIdentifier = "aaA" };
                session.Store(sample2);
                session.SaveChanges();

                docId2 = sample2.Id;
            }

            var key = "UniqueConstraints/" + "Docs".ToLowerInvariant() + "/" + "AlternateIdentifier".ToLowerInvariant() + "/"
                                           + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("aaa");

            using (var session = DocumentStore.OpenSession())
            {
                var doc = session.Load<UniqueConstraintExtensions.ConstraintDocument>(key);

                Assert.NotNull(doc);
                Assert.True(doc.Constraints.ContainsKey("aaa"));
                Assert.True(doc.Constraints.ContainsKey("aaA"));
                Assert.Equal(docId1, doc.Constraints["aaa"].RelatedId);
                Assert.Equal(docId2, doc.Constraints["aaA"].RelatedId);
            }

            DocumentStore.DatabaseCommands.Delete(docId2, null);

            using (var session = DocumentStore.OpenSession())
            {
                var doc = session.Load<UniqueConstraintExtensions.ConstraintDocument>(key); ;

                Assert.NotNull(doc);
                Assert.True(doc.Constraints.ContainsKey("aaa"));
                Assert.False(doc.Constraints.ContainsKey("aaA"));
                Assert.Equal(docId1, doc.Constraints["aaa"].RelatedId);
            }

            DocumentStore.DatabaseCommands.Delete(docId1, null);
            Assert.Null(DocumentStore.DatabaseCommands.Get(key));
        }
    }
}
