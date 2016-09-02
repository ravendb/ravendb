// -----------------------------------------------------------------------
//  <copyright file="ShardedFacets.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Xunit;

namespace SlowTests.MailingList
{
    public class ShardedFacets : RavenTestBase
    {
        protected override void ModifyStore(DocumentStore store)
        {
            store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
        }

        [Fact(Skip = "Missing feature: Facets")]
        public void FacetTest()
        {
            using (var ds1 = GetDocumentStore())
            using (var ds2 = GetDocumentStore())
            {
                var sharded = new ShardedDocumentStore(
                    new ShardStrategy(
                        new Dictionary<string, IDocumentStore> {
                        {"first", ds1},
                        {"second", ds2}
                    }));

                using (sharded.Initialize())
                {
                    using (var session = sharded.OpenSession())
                    {
                        session.Store(new Tag { Name = "tag1" });
                        session.Store(new Tag { Name = "tag1" });
                        session.Store(new Tag { Name = "tag2" });
                        session.SaveChanges();
                    }

                    using (var session = sharded.OpenSession())
                    {
                        session.Store(new Tag { Name = "tag3" });
                        session.Store(new Tag { Name = "tag5" });
                        session.Store(new Tag { Name = "tag8" });
                        session.SaveChanges();
                    }

                    new Tags_ByName().Execute(sharded);


                    WaitForIndexing(ds1);
                    WaitForIndexing(ds2);

                    using (var session = sharded.OpenSession())
                        Assert.NotEmpty(session
                            .Query<Tag, Tags_ByName>()
                            .ToFacets(new[] { new Facet { Name = "Name" } }).Results);
                }
            }
        }

        private class Tags_ByName : AbstractIndexCreationTask<Tag>
        {
            public Tags_ByName()
            {
                Map = tags =>
                      from tag in tags
                      select new { tag.Name };
            }
        }

        private class Tag
        {
            public string Name { get; set; }
        }
    }
}
