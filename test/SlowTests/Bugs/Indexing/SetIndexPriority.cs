// -----------------------------------------------------------------------
//  <copyright file="SetIndexPriority.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class SetIndexPriority : RavenTestBase
    {
        private class FakeIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition()
                {
                    Maps = { "from d in docs select new { d.Id }" }
                };
            }
        }

        [Fact]
        public void changing_index_priority_needs_to_set_it_on_index_instance_as_well()
        {
            using (var store = GetDocumentStore())
            {
                new FakeIndex().Execute(store);

                foreach (var expected in new[] { IndexPriority.Normal, IndexPriority.High, IndexPriority.Low })
                {
                    store.Admin.Send(new SetIndexPriorityOperation("FakeIndex", expected));

                    var db = GetDocumentDatabaseInstanceFor(store).Result;
                    var indexInstance = db.IndexStore.GetIndex("FakeIndex");

                    Assert.Equal(expected, indexInstance.Definition.Priority);
                }
            }
        }

        [Fact]
        public void set_auto_index_priority()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person()
                    {
                        Name = "Vasia"
                    });
                    session.SaveChanges();
                }
                QueryStatistics statistics;
                using (var session = store.OpenSession())
                {
                    session.Query<Person>().Customize(x => x.WaitForNonStaleResults()).Statistics(out statistics).Where(x=>x.Name == "Vasia").ToList();
                }
                
                store.Admin.Send(new SetIndexPriorityOperation(statistics.IndexName,IndexPriority.Low));
                var index = store.Admin.Send(new GetIndexOperation(statistics.IndexName));
                Assert.Equal(IndexPriority.Low, index.Priority);

                store.Admin.Send(new SetIndexPriorityOperation(statistics.IndexName, IndexPriority.High));
                index = store.Admin.Send(new GetIndexOperation(statistics.IndexName));
                Assert.Equal(IndexPriority.High, index.Priority);

                store.Admin.Send(new SetIndexPriorityOperation(statistics.IndexName, IndexPriority.Normal));
                index = store.Admin.Send(new GetIndexOperation(statistics.IndexName));
                Assert.Equal(IndexPriority.Normal, index.Priority);
            }
        }
    }
}
