using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Enzmann : RavenTestBase
    {
        [Fact]
        public void Include()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new EntityIndex());

                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Entity { Id = "1", SubEntitiesIds = new List<string> { } }); // Works when list is not empty
                    session.SaveChanges();
                }

                WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var works = session.Query<EntityIndex.Result, EntityIndex>()
                        .ProjectInto<EntityIndex.Result>()
                        .ToList();

                    // System.ArgumentException : Illegal path
                    var fails = session.Query<EntityIndex.Result, EntityIndex>()
                        .Include(x => x.SubEntities.Select(y => y.Id)) // Just an example, original code has a more complex Select()
                        .ProjectInto<EntityIndex.Result>()
                        .Single(x => x.Id == "1");
                }
            }
        }

        private class Entity
        {
            public string Id { get; set; }
            public List<string> SubEntitiesIds { get; set; } = new List<string>();
        }

        private class EntityIndex : AbstractIndexCreationTask<Entity, EntityIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public List<Result> SubEntities { get; set; }
            }

            public EntityIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Id = doc.Id,
                                  SubEntities = from subEntityId in doc.SubEntitiesIds
                                                select new
                                                {
                                                    Id = subEntityId,
                                                    SubEntities = new object[0]
                                                }
                              };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
