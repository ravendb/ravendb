using System;

using Raven.Client.UniqueConstraints;
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
				var sample1 = new Doc {AlternateIdentifier = "aaa#"};
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
            throw new NotImplementedException();
        }

        [Fact]
        public void Server_should_convert_old_unique_constraint_document()
        {
            throw new NotImplementedException();
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

            var doc = DocumentStore.DatabaseCommands.Get(key);

            Assert.NotNull(doc);
            Assert.True(doc.DataAsJson.ContainsKey("aaa"));
            Assert.True(doc.DataAsJson.ContainsKey("aaA"));
            Assert.Equal(docId1, doc.DataAsJson["aaa"].Value<string>("RelatedId"));
            Assert.Equal(docId2, doc.DataAsJson["aaA"].Value<string>("RelatedId"));

            DocumentStore.DatabaseCommands.Delete(docId2, null);

            doc = DocumentStore.DatabaseCommands.Get(key);

            Assert.NotNull(doc);
            Assert.True(doc.DataAsJson.ContainsKey("aaa"));
            Assert.False(doc.DataAsJson.ContainsKey("aaA"));
            Assert.Equal(docId1, doc.DataAsJson["aaa"].Value<string>("RelatedId"));

            DocumentStore.DatabaseCommands.Delete(docId1, null);

            doc = DocumentStore.DatabaseCommands.Get(key);

            Assert.Null(doc);
        }
	}
}
