using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Raven.Client;

namespace Raven.Tests.MailingList
{
	public class RavenDbBugs : RavenTest
	{
		[Fact]
		public void CanUseEnumInMultiMapTransform()
		{
			using (var store = NewDocumentStore())
			{
				new TestIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Cat { Name = "Kitty" }, "cat/kitty");
					session.Store(new Duck { Name = "Ducky" }, "duck/ducky");
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<Animal, TestIndex>()
						.ProjectFromIndexFieldsInto<Animal>()
						.Customize(t => t.WaitForNonStaleResultsAsOfNow())
						.ToList();
					Assert.NotEmpty(result);
				}

			}
		}

		class Cat
		{
			public string Name { get; set; }
		}

		class Duck
		{
			public string Name { get; set; }
		}
		class Animal
		{
			public string Alias { get; set; }
			public string Name { get; set; }
			public AnimalClass Type { get; set; }
		}

		enum AnimalClass
		{
			Has4Legs,
			Has2Legs
		}
		class TestIndex : AbstractMultiMapIndexCreationTask<Animal>
		{
			public TestIndex()
			{
				AddMap<Cat>(cats => from cat in cats
									select new
									{
										Name = cat.Name,
										Alias = cat.Name,
										Type = AnimalClass.Has4Legs
									});
				AddMap<Duck>(ducks => from duck in ducks
									  select new
									  {
										  Name = duck.Name,
										  Alias = duck.Name,
										  Type = AnimalClass.Has2Legs
									  });
				TransformResults = (database, animals) =>
								   from animal in animals
								   select new
								   {
									   Name = animal.Name,
									   Alias = animal.Alias,
									   Type = animal.Type // Comment out this line and all works fine
								   };

				Store(x=>x.Alias, FieldStorage.Yes);
				Store(x=>x.Type, FieldStorage.Yes);
			}
		}

	}
}