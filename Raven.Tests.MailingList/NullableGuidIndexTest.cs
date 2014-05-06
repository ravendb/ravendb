using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class NullableGuidIndexTest : RavenTest
	{
		public class TestDocument
		{
			public string Id { get; set; }

			public Guid? OptionalExternalId { get; set; }
		}

		public class TestDocumentIndex : AbstractIndexCreationTask<TestDocument>
		{
			public TestDocumentIndex()
			{
				Map = docs => from doc in docs
							  where doc.OptionalExternalId != null
							  select new { doc.OptionalExternalId };
			}
		}

		[Fact]
		public void Can_query_against_nullable_guid()
		{
			using (var store = NewDocumentStore())
			{
				new TestDocumentIndex().Execute(store.DatabaseCommands, store.Conventions);

				using (var session = store.OpenSession())
				{
					session.Store(new TestDocument());
					session.Store(new TestDocument { OptionalExternalId = Guid.NewGuid() });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					TestDocument[] results = session.Query<TestDocument, TestDocumentIndex>()
						.Customize(c => c.WaitForNonStaleResultsAsOfLastWrite())
						.ToArray();
					Assert.Empty(store.DocumentDatabase.Statistics.Errors);
					Assert.NotEmpty(results);
				}
			}
		}
	}
}