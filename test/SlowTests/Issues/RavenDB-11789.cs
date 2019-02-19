using System;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11789 : RavenTestBase
    {

        public class Entity
        {
            public string Id { get; set; }
            public string Description { get; set; }
        }

        public class EntityIndex : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<Entity>
        {
            public EntityIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  doc.Description
                              };

                Index(r => r.Description, Raven.Client.Documents.Indexes.FieldIndexing.Search);
            }
        }


        public class Entity2
        {
            public string Id { get; set; }
            public string Description { get; set; }
        }

        public class Entity2Index : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<Entity2>
        {
            public Entity2Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  doc.Description
                              };

                Index(r => r.Description, Raven.Client.Documents.Indexes.FieldIndexing.Search);
            }
        }

        [Fact]
        public void DeleteCorruption()
        {
            using (var store = GetDocumentStore())
            {
                new EntityIndex().Execute(store);
                new Entity2Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Entity());
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var operation1 = session.Advanced.DocumentStore.Operations.Send(new Raven.Client.Documents.Operations.DeleteByQueryOperation<Entity, EntityIndex>(r => r.Id.Any()));
                    var operation2 = session.Advanced.DocumentStore.Operations.Send(new Raven.Client.Documents.Operations.DeleteByQueryOperation<Entity2, Entity2Index>(r => r.Id.Any()));
                    operation1.WaitForCompletion();
                    operation2.WaitForCompletion();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Entity());
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    try
                    {
                        session.SaveChanges();
                    }
                    catch (Exception)
                    {
                        WaitForUserToContinueTheTest(store);

                        throw;
                    }
                }


            }
        }
    }
}
