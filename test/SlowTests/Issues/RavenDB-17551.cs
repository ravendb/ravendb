﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17551 : RavenTestBase
    {
        public RavenDB_17551(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void CanUseOffsetWithCollectionQuery(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var s = store.OpenSession())
            {
                for (int i = 0; i < 5; i++)
                {
                    s.Store(new { i = i }, "items/");
                }
                s.SaveChanges();
            }

            using(var session = store.OpenSession())
            {
                Assert.Equal(3, session.Query<object>().Take(3).Skip(2).ToList().Count);
                Assert.Equal(2, session.Query<object>().Take(3).Skip(3).ToList().Count);
            }
        }
    }
}
