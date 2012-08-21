using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Vlko
{
	public class RelationIdIndex : RavenTest
	{
		[Fact]
		public void CanBeUsedForTransformResultsWithDocumentId()
		{
			using (var store = NewDocumentStore())
			{
				new ThorIndex().Execute(((IDocumentStore) store).DatabaseCommands, ((IDocumentStore) store).Conventions);

				var relId = Guid.NewGuid();
				using (var s = store.OpenSession())
				{
					s.Store(new Thor
					{
						Id = Guid.NewGuid(),
						Name = "Thor",
						Rel = new Relation
						{
							Id = relId
						}
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var objects = s.Query<Thor, ThorIndex>()
							.Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(3)))
							.ToArray();
					Assert.Equal(1, objects.Length);
					Assert.Equal(relId, objects[0].Rel.Id);

					objects = s.Query<Thor, ThorIndex>()
							.Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(3)))
							.Where(item => item.Rel.Id == relId)
							.ToArray();

					Assert.Equal(1, objects.Length);
					Assert.Equal(relId, objects[0].Rel.Id);
				}
			}
		}


		public class ThorIndex : AbstractIndexCreationTask<Thor>
		{
			public ThorIndex()
			{
				Map = thors => from doc in thors
							   select new { doc.Name, Rel_Id = doc.Rel.Id };
			}
		}

		public class Thor
		{
			public Guid Id { get; set; }
			public string Name { get; set; }
			public Relation Rel { get; set; }
		}

		public class Relation
		{
			public Guid Id { get; set; }
		}
	}


}