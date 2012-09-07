using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;
using System;

namespace Raven.Tests.Bugs.Zhang
{
	public class UseMaxForDateTimeTypeInReduce : RavenTest
	{
		private const string map = @"
from doc in docs
from tag in doc.Tags
select new { Name = tag.Name, CreatedTime = doc.CreatedTime.Ticks }
";

		private const string reduce = @"
from agg in results
group agg by agg.Name into g
let createdTime = g.Max(x => (long)x.CreatedTime)
select new {Name = g.Key, CreatedTime = createdTime}
";

		[Fact]
		public void CanUseMax()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map = map,
													Reduce = reduce,
												});

				using (var sesion = store.OpenSession())
				{
					sesion.Store(new { Topic = "RavenDB is Hot", CreatedTime = SystemTime.UtcNow, Tags = new[] { new { Name = "DB" }, new { Name = "NoSQL" } } });

					sesion.Store(new { Topic = "RavenDB is Fast", CreatedTime = SystemTime.UtcNow.AddMinutes(10), Tags = new[] { new { Name = "NoSQL" } } });

					sesion.SaveChanges();
				}

				using (var sesion = store.OpenSession())
				{
					sesion.Advanced.LuceneQuery<object>("test").WaitForNonStaleResults().ToArray<object>();
				}

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
	}
}
