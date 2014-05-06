using System;
using System.Linq;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryingOverTags : RavenTest
	{
		[Fact]
		public void Can_chain_wheres_when_querying_collection_with_any()
		{
			var entity = new EntityWithTags()
			{
				Id = Guid.NewGuid(),
				Tags = new [] { "FOO", "BAR" }
			};
			using(var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(entity);
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					IQueryable<EntityWithTags> query = session.Query<EntityWithTags>()
						.Customize(x=>x.WaitForNonStaleResultsAsOfLastWrite());

					foreach (var tag in new string[] { "FOO", "BAR" })
					{
						string localTag = tag;
						query = query.Where(e => e.Tags.Any(t => t == localTag));
					}

					Assert.NotEmpty(query.ToList());
				}
			}
		}

		public class EntityWithTags
		{
			public Guid Id { get; set; }
			public System.Collections.Generic.IEnumerable<string> Tags
			{
				get;
				set;
			}
		}
	}
}