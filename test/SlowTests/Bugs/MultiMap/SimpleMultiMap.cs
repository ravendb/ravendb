using System;
using FastTests;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Bugs.MultiMap
{
    public class SimpleMultiMap : RavenTestBase
    {
        [Fact]
        public void CanCreateMultiMapIndex()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.PrettifyGeneratedLinqExpressions = false
            }))
            {
                new CatsAndDogs().Execute(store);

                var indexDefinition = store.Maintenance.Send(new GetIndexOperation("CatsAndDogs"));

                Assert.Equal(2, indexDefinition.Maps.Count);

                RavenTestHelper.AssertEqualRespectingNewLines(@"docs.Cats.Select(cat => new {
    Name = cat.Name
})", indexDefinition.Maps.First());

                RavenTestHelper.AssertEqualRespectingNewLines(@"docs.Dogs.Select(dog => new {
    Name = dog.Name
})", indexDefinition.Maps.Skip(1).First());
            }
        }

        [Fact]
        public void CanQueryUsingMultiMap()
        {
            using (var store = GetDocumentStore())
            {
                new CatsAndDogs().Execute(store);

                using (var documentSession = store.OpenSession())
                {
                    documentSession.Store(new Cat { Name = "Tom" });
                    documentSession.Store(new Dog { Name = "Oscar" });
                    documentSession.SaveChanges();
                }

                using (var session = store.OpenSession())
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

        private class CatsAndDogs : AbstractMultiMapIndexCreationTask
        {
            public CatsAndDogs()
            {
                AddMap<Cat>(cats => from cat in cats
                                    select new { cat.Name });

                AddMap<Dog>(dogs => from dog in dogs
                                    select new { dog.Name });
            }
        }

        private interface IHaveName
        {
            string Name { get; }
        }

        private class Cat : IHaveName
        {
            public string Name { get; set; }
        }

        private class Dog : IHaveName
        {
            public string Name { get; set; }
        }
    }


}
