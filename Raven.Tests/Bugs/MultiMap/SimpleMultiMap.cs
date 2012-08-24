using System;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.MultiMap
{
	public class SimpleMultiMap : RavenTest
	{
		[Fact]
		public void CanCreateMultiMapIndex()
		{
			using(var store = NewDocumentStore())
			{
				new CatsAndDogs().Execute(store);

				var indexDefinition = store.DatabaseCommands.GetIndex("CatsAndDogs");
				Assert.Equal(2, indexDefinition.Maps.Count);
				Assert.Equal(@"docs.Cats.Select(cat => new {
    Name = cat.Name
})", indexDefinition.Maps.First());
				Assert.Equal(@"docs.Dogs.Select(dog => new {
    Name = dog.Name
})", indexDefinition.Maps.Skip(1).First());   
			}
		}

		[Fact]
		public void CanQueryUsingMutliMap()
		{
			using (var store = NewDocumentStore())
			{
				new CatsAndDogs().Execute(store);

				using(var documentSession = store.OpenSession())
				{
					documentSession.Store(new Cat{Name = "Tom"});
					documentSession.Store(new Dog{Name = "Oscar"});
					documentSession.SaveChanges();
				}

				using(var session = store.OpenSession())
				{
					var haveNames = session.Query<IHaveName, CatsAndDogs>()
						.Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
						.OrderBy(x => x.Name)
						.ToList();

					Assert.Equal(2, haveNames.Count);
					Assert.IsType<Dog>(haveNames[0]);
					Assert.IsType<Cat>(haveNames[1]);
				}
			}
		}

		public class CatsAndDogs : AbstractMultiMapIndexCreationTask
		{
			public CatsAndDogs()
			{
				AddMap<Cat>(cats => from cat in cats
				                 select new {cat.Name});

				AddMap<Dog>(dogs => from dog in dogs
								 select new { dog.Name });
			}
		}

		public interface IHaveName
		{
			string Name { get; }
		}

		public class Cat : IHaveName
		{
			public string Name { get; set; }
		}

		public class Dog : IHaveName
		{
			public string Name { get; set; }
		}
	}

	
}