using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.MultiMap
{
	public class SimpleMultiMap : LocalClientTest
	{
		[Fact]
		public void CanCreateMultiMapIndex()
		{
			using(var store = NewDocumentStore())
			{
				new CatsAndDogs().Execute(store);

				var indexDefinition = store.DatabaseCommands.GetIndex("CatsAndDogs");
				Assert.Equal(2, indexDefinition.Maps.Count);
				Assert.Equal("docs.Cats\r\n\t.Select(cat => new {Name = cat.Name})", indexDefinition.Maps[0]);
				Assert.Equal("docs.Dogs\r\n\t.Select(dog => new {Name = dog.Name})", indexDefinition.Maps[1]);   
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

		public class Cat
		{
			public string Name { get; set; }
		}

		public class Dog
		{
			public string Name { get; set; }
		}
	}

	
}