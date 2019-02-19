using System;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class AddMapForAllTest : RavenTestBase
    {
        // Parent class whose children will be indexed.
        private abstract class Animal
        {
            public string Name { get; set; }
        }

        private class Rhino : Animal
        {
        }

        private class Tiger : Animal
        {
        }

        private abstract class Equine : Animal
        {
        }

        private class Horse : Equine
        {
        }

        private class AnimalsByName : AbstractMultiMapIndexCreationTask<Animal>
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

        [Fact]
        public void IndexOnAbstractParentIndexesChildClasses()
        {
            using (var store = GetDocumentStore())
            {
                new AnimalsByName().Execute(store);

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
