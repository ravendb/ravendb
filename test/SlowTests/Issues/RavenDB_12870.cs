using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12870 : RavenTestBase
    {
        [Fact]
        public void Should_handle_map_reduce_index_tombstones_update_delete()
        {
            using (var store = GetDocumentStore())
            {
                var mapReduceIndex = new Toss_TagPerDay();
                mapReduceIndex.Execute(store);

                using (var session = store.OpenSession())
                {
                    var entity = new TossEntity()
                    {
                        Tags = new List<string>()
                        {
                            "raven",
                            "nosql"
                        },
                        CreatedOn = DateTime.Now.AddDays(-1)
                    };
                    session.Store(entity);

                    session.SaveChanges();

                    WaitForIndexing(store);

                    var mapReduceResults = session
                        .Query<TagByDayIndex, Toss_TagPerDay>()
                        .ToList();

                    Assert.Equal(2, mapReduceResults.Count);

                    store.Maintenance.Send(new DisableIndexOperation(mapReduceIndex.IndexName));

                    entity.CreatedOn = DateTime.Now;
                    session.SaveChanges();

                    session.Delete(entity);
                    session.SaveChanges();

                    store.Maintenance.Send(new EnableIndexOperation(mapReduceIndex.IndexName));

                    WaitForIndexing(store);

                    mapReduceResults = session
                        .Query<TagByDayIndex, Toss_TagPerDay>()
                        .ToList();
                    Assert.Equal(0, mapReduceResults.Count);
                }
            }
        }

        [Fact]
        public void Should_handle_map_index_tombstones_update_delete()
        {
            using (var store = GetDocumentStore())
            {
                var mapIndex = new Toss_ByCreatedOn();
                mapIndex.Execute(store);

                using (var session = store.OpenSession())
                {
                    var entity = new TossEntity()
                    {
                        Tags = new List<string>()
                        {
                            "raven",
                            "nosql"
                        },
                        CreatedOn = DateTime.Now.AddDays(-1)
                    };
                    session.Store(entity);

                    session.SaveChanges();

                    WaitForIndexing(store);

                    var mapResults = session.Query<TossEntity, Toss_ByCreatedOn>().ToList();

                    Assert.Equal(1, mapResults.Count);

                    store.Maintenance.Send(new DisableIndexOperation(mapIndex.IndexName));

                    entity.CreatedOn = DateTime.Now;
                    session.SaveChanges();

                    session.Delete(entity);
                    session.SaveChanges();

                    store.Maintenance.Send(new EnableIndexOperation(mapIndex.IndexName));

                    WaitForIndexing(store);

                    mapResults = session.Query<TossEntity, Toss_ByCreatedOn>().Statistics(out var stats).ToList();
                    Assert.Equal(0, mapResults.Count);
                    Assert.Equal(0, stats.TotalResults);
                }
            }
        }

        [Fact]
        public void Should_handle_map_index_tombstones_delete_and_put()
        {
            using (var store = GetDocumentStore())
            {
                var mapIndex = new Toss_ByCreatedOn();
                mapIndex.Execute(store);

                using (var session = store.OpenSession())
                {
                    var entity = new TossEntity()
                    {
                        Tags = new List<string>()
                        {
                            "raven",
                            "nosql"
                        },
                        CreatedOn = DateTime.Now.AddDays(-1)
                    };
                    session.Store(entity);

                    session.SaveChanges();

                    WaitForIndexing(store);

                    var mapResults = session.Query<TossEntity, Toss_ByCreatedOn>().ToList();

                    Assert.Equal(1, mapResults.Count);

                    store.Maintenance.Send(new DisableIndexOperation(mapIndex.IndexName));

                    session.Delete(entity);
                    session.SaveChanges();

                    session.Store(entity, "TossEntities/1-A");
                    session.SaveChanges();

                    store.Maintenance.Send(new EnableIndexOperation(mapIndex.IndexName));

                    WaitForIndexing(store);

                    mapResults = session.Query<TossEntity, Toss_ByCreatedOn>().Statistics(out var stats).ToList();
                    Assert.Equal(1, mapResults.Count);
                    Assert.Equal(2, stats.TotalResults);
                }
            }
        }

        private class TossEntity
        {
            public DateTime CreatedOn { get; set; }
            public List<string> Tags { get; set; }
        }

        private class Toss_TagPerDay : AbstractIndexCreationTask<TossEntity, TagByDayIndex>
        {
            public Toss_TagPerDay()
            {

                Map = tosses => from toss in tosses
                                from tag in toss.Tags
                                select new TagByDayIndex()
                                {
                                    Tag = tag,
                                    CreatedOn = toss.CreatedOn.Date,
                                    Count = 1
                                };
                Reduce = results => from result in results
                                    group result by new { result.Tag, result.CreatedOn }
                    into g
                                    select new TagByDayIndex()
                                    {
                                        Tag = g.Key.Tag,
                                        CreatedOn = g.Key.CreatedOn,
                                        Count = g.Sum(i => i.Count)
                                    };
            }
        }

        private class Toss_ByCreatedOn : AbstractIndexCreationTask<TossEntity>
        {
            public Toss_ByCreatedOn()
            {

                Map = tosses => from toss in tosses
                                from tag in toss.Tags
                                select new TagByDayIndex()
                                {
                                    Tag = tag,
                                    CreatedOn = toss.CreatedOn.Date
                                };
            }
        }

        private class TagByDayIndex
        {
            public string Tag { get; set; }
            public DateTime CreatedOn { get; set; }
            public int Count { get; set; }
        }
    }
}
