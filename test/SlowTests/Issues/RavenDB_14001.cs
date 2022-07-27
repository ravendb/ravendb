using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14001 : RavenTestBase
    {
        public RavenDB_14001(ITestOutputHelper output) : base(output)
        {
        }

        private class Thing
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class ThingIndex : AbstractMultiMapIndexCreationTask<ThingIndex.Result>
        {
            public class Result
            {
                public string Type { get; set; }
                public string Name { get; set; }
            }

            public ThingIndex()
            {
                //"Raven-Clr-Type": "MyProject.Thing, MyProject.Model"
                AddMapForAll<Thing>(things => from thing in things
                                              let clr = (string)MetadataFor(thing)["Raven-Clr-Type"]
                                              let tn = clr.Substring(0, clr.IndexOf(','))
                                              let type = tn.Substring(tn.LastIndexOf('.') + 1)
                                              select new
                                              {
                                                  Type = type,
                                                  thing.Name,
                                              });
            }
        }

        [Fact]
        public void CollectionNameWithDashesCanBeIndexedWithMetadata()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = a => a.Conventions.FindCollectionName = t => string.Join("-", t.Name.ToCharArray())
            }))
            {
                new ThingIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Thing
                    {
                        Name = "John"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var things = session
                        .Query<Thing, ThingIndex>()
                        .Customize(x => x.NoCaching())
                        .Where(x => x.Name == "John")
                        .ToList();

                    Assert.Equal(1, things.Count);
                    Assert.Equal("John", things[0].Name);
                }
            }
        }
    }
}
