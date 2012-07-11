using System;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class AddMapForAllTest : RavenTest
	{
		// Parent class whose children will be indexed.
		public abstract class Animal
		{
			public string Name { get; set; }
		}

		public class Rhino : Animal
		{
		}

		public class Tiger : Animal
		{
		}

		public abstract class Equine : Animal
		{
		}

		public class Horse : Equine
		{
		}

		public class AnimalsByName : AbstractMultiMapIndexCreationTask<Animal>
		{
			public AnimalsByName()
			{
				AddMapForAll<Animal>(parents =>
									 from parent in parents
									 select new
									 {
										 parent.Name
									 });
			}
		}

		protected override void CreateDefaultIndexes(IDocumentStore documentStore)
		{
			base.CreateDefaultIndexes(documentStore);
			new AnimalsByName().Execute(documentStore);
		}

		[Fact]
		public void IndexOnAbstractParentIndexesChildClasses()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Rhino { Name = "Ronald" });
					session.Store(new Rhino { Name = "Roger" });
					session.Store(new Tiger { Name = "Tina" });
					session.Store(new Horse { Name = "Mahoney" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(2, GetAnimals(session, x => x.Name.StartsWith("R")).Length);
					Assert.Equal(1, GetAnimals(session, x => x.Name == "Tina").Length);

					// Check that indexing applies to more than a single level of inheritance.
					Assert.IsType<Horse>(GetAnimals(session, x => x.Name == "Mahoney").Single());
				}
			}
		}

		private static Animal[] GetAnimals(IDocumentSession s, Expression<Func<Animal, bool>> e)
		{
			return s.Query<Animal, AnimalsByName>().Customize(x => x.WaitForNonStaleResults()).Where(e).ToArray();
		}
	}
}
