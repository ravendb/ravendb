using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13498 : RavenTestBase
    {
        public RavenDB_13498(ITestOutputHelper output) : base(output)
        {
        }

        public class Thing
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class ThingIndex : AbstractIndexCreationTask<Thing>
        {
            public ThingIndex()
            {
                Map = things => from thing in things select new { thing.Name };
            }
        }

        public class ThingIndex2 : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from d in docs[""T-h-i-n-g""] select new { d.Name } "
                    }
                };
            }
        }


        [Fact]
        public void CollectionNameWithDashesCanBeIndexed()
        {
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = a => a.Conventions.FindCollectionName = t => String.Join("-", t.Name.ToCharArray())
            }))
            {
                IndexCreation.CreateIndexes(new AbstractIndexCreationTask[] { new ThingIndex(), new ThingIndex2() }, store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Thing { Name = "Oren" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Thing, ThingIndex>().Where(x => x.Name == "Oren").ToList());
                    Assert.NotEmpty(session.Query<Thing, ThingIndex2>().Where(x => x.Name == "Oren").ToList());
                }
            }
        }
    }
}
