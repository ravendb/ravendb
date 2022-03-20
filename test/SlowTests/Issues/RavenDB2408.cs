// -----------------------------------------------------------------------
//  <copyright file="RavenDB2408.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB2408 : RavenTestBase
    {
        public RavenDB2408(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Document_id_not_null_when_projecting()
        {
            using (var store = GetDocumentStore())
            {
                new TimeoutsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Timeout
                    {
                        Owner = string.Empty,
                        Time = DateTime.UtcNow,
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Timeout, TimeoutsIndex>()
                        .Where(t => t.Owner == string.Empty)
                        .OrderBy(t => t.Time)
                        .Select(t => t.Time);

                    using (var enumerator = session.Advanced.Stream(query))
                    {
                        while (enumerator.MoveNext())
                        {
                            Assert.NotNull(enumerator.Current.Id);
                        }
                    }
                }
            }
        }

        private class Timeout
        {
            public string Id { get; set; }
            public DateTime Time { get; set; }
            public string Owner { get; set; }
        }

        private class TimeoutsIndex : AbstractIndexCreationTask<Timeout>
        {
            public TimeoutsIndex()
            {
                Map = docs => from d in docs
                              select new Timeout { Owner = d.Owner, Time = d.Time };
            }
        }
    }
}
