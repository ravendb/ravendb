// -----------------------------------------------------------------------
//  <copyright file="SafeGaz.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class SafeGaz : RavenTest
    {
        [Fact]
        public void Test()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.DefaultQueryingConsistency =
                    ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite;
                new EntityIndex().Execute(store);
                Guid id = Guid.NewGuid();

                using (var session = store.OpenSession())
                {
                    Entity e = new Entity { Id = id, Ids = new List<Guid> { Guid.NewGuid() } };
                    session.Store(e);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(session.Query<Entity>().Count(), 1);
                }

                using (var session = store.OpenSession())
                {
                    var e = session.Load<Entity>(id);
                    e.Ids.Remove(e.Ids.First());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var count = session.Query<Entity>().Statistics(out stats).Count();
                    Assert.False(stats.IsStale);

                    // FAILS HERE
                    Assert.Equal(count, 1);
                }
            }
        }


        public class Entity
        {
            public Guid Id { get; set; }

            public IList<Guid> Ids { get; set; }
        }

        public class EntityIndex : AbstractIndexCreationTask<Entity>
        {
            public EntityIndex()
            {
                Map = docs => docs.Select(x => new Definition { Ids = x.Ids, });
            }

            public class Definition
            {
                public IEnumerable<Guid> Ids { get; set; }
            }
        }
    }
}