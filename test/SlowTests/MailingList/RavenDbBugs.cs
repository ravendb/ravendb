using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class RavenDbBugs : RavenTestBase
    {
        [Fact]
        public void CanUseEnumInMultiMapTransform()
        {
            using (var store = GetDocumentStore())
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

        private class Cat
        {
            public string Name { get; set; }
        }

        private class Duck
        {
            public string Name { get; set; }
        }
        private class Animal
        {
            public string Alias { get; set; }
            public string Name { get; set; }
            public AnimalClass Type { get; set; }
        }

        private enum AnimalClass
        {
            Has4Legs,
            Has2Legs
        }

        private class TestIndex : AbstractMultiMapIndexCreationTask<Animal>
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


                Store(x => x.Alias, FieldStorage.Yes);
                Store(x => x.Type, FieldStorage.Yes);
            }
        }

    }
}
