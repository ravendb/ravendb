// -----------------------------------------------------------------------
//  <copyright file="Garrett.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Garrett : RavenTestBase
    {
        public Garrett(ITestOutputHelper output) : base(output)
        {
        }

        private class StrategyIndividual
        {
            public string OtherProp { get; set; }
            public Dictionary<int, double> Statistcs { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanOrderByDictionaryValue(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new StrategyIndividual
                    {
                        OtherProp = "Test",
                        Statistcs = new Dictionary<int, double>
                        {
                            { 4, 5.0 }
                        }
                    });

                    session.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Query<StrategyIndividual>()
                     .Where(x => x.Statistcs[4] == 0)
                     .OrderBy(x => x.Statistcs[4])
                     .ToList();
                }
            }
        }
    }
}
