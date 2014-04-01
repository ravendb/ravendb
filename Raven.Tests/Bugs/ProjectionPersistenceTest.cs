using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class ProjectionPersistenceTest : RavenTest
	{
		public class Spell
		{
			public string Name { get; set; }
			public string Id { get; set; }
			public int Level { get; set; }
			public int Cost { get; set; }
			public string Description { get; set; }
			public string Effects { get; set; }
		}

		public class SpellViewModel
		{
			public string Name { get; set; }
			public string Id { get; set; }
			public int Cost { get; set; }
		}

		private class SpellByName : AbstractIndexCreationTask<Spell, SpellByName.Result>
		{
			public class Result
			{
				public string Id { get; set; }
				public string Name { get; set; }
				public int Cost { get; set; }
			}

			public SpellByName()
			{
				Map = spells => from spell in spells
				                select new Result
				                {
					                Id = spell.Id,
					                Name = spell.Name,
					                Cost = spell.Cost
				                };
			}
		}

		[Fact]
		public void ShouldThrowAnException()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var spell = new Spell
					{
						Name = "First Level Spell",
						Cost = 100,
						Description = "Test Description",
						Effects = "Test Effects",
						Level = 1
					};
					session.Store(spell);
					session.SaveChanges();

					Assert.Equal(spell.Id, "spells/1");
					Assert.Equal(spell.Cost, 100);
				}

				new SpellByName().Execute(store);

				using (var session = store.OpenSession())
				{
					var exception = Assert.Throws<InvalidOperationException>(() =>
					{
						var results = session.Query<SpellByName.Result, SpellByName>()
							.Customize(customization => customization.WaitForNonStaleResults())
							.ToList();
					});
					Assert.Equal("The query results type is 'Spell' but you expected to get results of type 'Result'. If you want to return a projection, you should use .AsProjection<Result>() before calling to .ToList().", exception.Message);
				}
			}
		}

		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var spell = new Spell
					{
						Name = "First Level Spell",
						Cost = 100,
						Description = "Test Description",
						Effects = "Test Effects",
						Level = 1
					};
					session.Store(spell);
					session.SaveChanges();

					Assert.Equal(spell.Id, "spells/1");
					Assert.Equal(spell.Cost, 100);
				}

				new SpellByName().Execute(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<SpellByName.Result, SpellByName>()
						.Customize(customization => customization.WaitForNonStaleResults())
						.AsProjection<SpellViewModel>()
						.ToList();

					Assert.Equal(results[0].Cost, 100);
				}
			}
		}
	}
}