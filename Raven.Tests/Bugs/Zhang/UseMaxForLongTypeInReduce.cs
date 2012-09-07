using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;
using System;

namespace Raven.Tests.Bugs.Zhang
{
	public class UseMaxForLongTypeInReduce : RavenTest
	{
		private const string map = @"
from doc in docs
from tag in doc.Tags
select new { Name = tag.Name, CreatedTimeTicks = doc.CreatedTimeTicks }
";

		private const string reduce = @"
from agg in results
group agg by agg.Name into g
let createdTimeTicks = g.Max(x => (long)x.CreatedTimeTicks)
select new {Name = g.Key, CreatedTimeTicks = createdTimeTicks}
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
					sesion.Store(new { Topic = "RavenDB is Hot", CreatedTimeTicks = SystemTime.UtcNow.Ticks, Tags = new[] { new { Name = "DB" }, new { Name = "NoSQL" } } });

					sesion.Store(new { Topic = "RavenDB is Fast", CreatedTimeTicks = SystemTime.UtcNow.AddMinutes(10).Ticks, Tags = new[] { new { Name = "NoSQL" } } });

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
