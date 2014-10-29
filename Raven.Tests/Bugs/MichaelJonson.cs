using System;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class MichaelJonson : RavenTest
	{
		[Fact]
		public void CanQueryAndIncludeFromMapReduceIndex()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				new MapReduceIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					for (var index = 0; index < 10; index++)
					{
						session.Store(new Entity("Name" + index, index % 2 == 0));
					}

					session.SaveChanges();
				}

				// force a wait until indexing is done
				using (var session = store.OpenSession())
				{
					session.Query<EntityStatisticResult, MapReduceIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var session = store.OpenSession())
				{
					var result = session
					  .Query<EntityStatisticResult, MapReduceIndex>()
					  .Customize(x => x.Include("Id"))
					  .Where(x => x.Name == "Name0")
					  .FirstOrDefault();

					Assert.NotNull(result);

					var user = session
						.Load<Entity>("entities/" + "Name0");
					Assert.NotNull(user);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}
			}
		}

		public class Entity
		{
			public Entity(string name, bool isIt)
			{
				Name = name;
				IsIt = isIt;
			}

			public string Id { get { return "entities/" + Name; } }
			public string Name { get; private set; }
			public bool IsIt { get; private set; }
		}

		public class MapReduceIndex : AbstractIndexCreationTask<Entity, EntityStatisticResult>
		{
			public MapReduceIndex()
			{
				Map = docs => from doc in docs
							  select new
							  {
								  Id = doc.Id,
								  Name = doc.Name,
								  Nots = (!doc.IsIt) ? 1 : 0,
								  IsSo = (doc.IsIt) ? 1 : 0
							  };

				Reduce = results => from result in results
									group result by result.Id
										into g
										select new
										{
											Id = g.Key,
											Name = g.First().Name,
											Nots = g.Sum(x => x.Nots),
											IsSo = g.Sum(x => x.IsSo)
										};

			}
		}

		public class EntityStatisticResult
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public int Nots { get; set; }
			public int IsSo { get; set; }
		}
	}
}