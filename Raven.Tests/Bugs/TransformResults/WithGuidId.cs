using System;
using System.Linq;
using Raven.Client;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs.TransformResults
{
	public class WithGuidId : RavenTest
	{
		[Fact]
		public void CanBeUsedForTransformResultsWithDocumentId()
		{
			using(var store = NewDocumentStore())
			{
				store.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (o, type, allowNull) => o.ToString();
				new ThorIndex().Execute(((IDocumentStore) store).DatabaseCommands, ((IDocumentStore) store).Conventions);

				using(var s = store.OpenSession())
				{
					s.Store(new Thor
					{
						Id = Guid.NewGuid(),
						Name = "Thor"
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var objects = s.Query<Thor>("ThorIndex")
						.Customize(x=>x.WaitForNonStaleResults())
						.ToArray();

					Assert.NotEqual(Guid.Empty,objects[0].Id);
				}
			}
		}
	}
}
