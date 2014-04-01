using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.MultiMap
{
	public class MultiMapWithCustomProperties : RavenTest
	{
		[Fact]
		public void Can_create_index()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Cat { Name = "Tom", CatsOnlyProperty = "Miau" });
					session.Store(new Dog { Name = "Oscar" });

					session.SaveChanges();
				}

				new CatsAndDogs().Execute(store);

				WaitForIndexing(store);

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);

				using (var s = store.OpenSession())
				{
					Assert.NotEmpty(s.Query<Cat, CatsAndDogs>()
										.Where(x => x.CatsOnlyProperty == "Miau")
										.ToList());

					Assert.NotEmpty(s.Query<Dog, CatsAndDogs>()
										.Where(x => x.Name == "Oscar")
										.ToList());

				}
			}
		}

		public class CatsAndDogs : AbstractMultiMapIndexCreationTask
		{
			public CatsAndDogs()
			{
				AddMap<Cat>(cats => from cat in cats
									select new { cat.Name, cat.CatsOnlyProperty });

				AddMap<Dog>(dogs => from dog in dogs
									select new { dog.Name, CatsOnlyProperty = (string)null });
			}
		}

		public interface IHaveName
		{
			string Name { get; }
		}

		public class Cat : IHaveName
		{
			public string Name { get; set; }
			public string CatsOnlyProperty { get; set; }
		}

		public class Dog : IHaveName
		{
			public string Name { get; set; }
		}
	}
}