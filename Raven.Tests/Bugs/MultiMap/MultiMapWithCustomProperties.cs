using System.Linq;
using Raven.Client.Indexes;
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
                    session.Store(new Cat { Name = "Tom", CatsOnlyProperty = "Miau"});
                    session.Store(new Dog { Name = "Oscar" });

                    session.SaveChanges();
                }

                new CatsAndDogs().Execute(store);

                WaitForIndexing(store);

                Assert.Empty(store.DocumentDatabase.Statistics.Errors);
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