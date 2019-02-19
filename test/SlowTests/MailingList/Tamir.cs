// -----------------------------------------------------------------------
//  <copyright file="Tamir.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class Tamir : RavenTestBase
    {
        private class Developer
        {
            public string Name { get; set; }
            public IDE PreferredIDE { get; set; }
        }

        private class IDE
        {
            public string Name { get; set; }
        }

        [Fact]
        public void InOnObjects()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = "DevByIDE",
                    Maps = { @"from dev in docs.Developers select new { dev.PreferredIDE, dev.PreferredIDE.Name }" }
                }}));

                using (var session = store.OpenSession())
                {

                    IEnumerable<Developer> developers = from name in new[] { "VisualStudio", "Vim", "Eclipse", "PyCharm" }
                                                        select new Developer
                                                        {
                                                            Name = string.Format("Fan of {0}", name),
                                                            PreferredIDE = new IDE
                                                            {
                                                                Name = name
                                                            }
                                                        };

                    foreach (var developer in developers)
                    {
                        session.Store(developer);
                    }

                    session.SaveChanges();

                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var bestIDEsEver = new[] { new IDE { Name = "VisualStudio" }, new IDE { Name = "IntelliJ" } };

                    QueryStatistics stats;
                    // this query returns with results
                    var querySpecificIDE = session.Query<Developer>().Statistics(out stats)
                                                  .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5)))
                                                  .Where(d => d.PreferredIDE.Name == "VisualStudio")
                                                  .ToList();

                    Assert.NotEmpty(querySpecificIDE);

                    // this query returns empty
                    var queryUsingWhereIn = session.Query<Developer>("DevByIDE").Statistics(out stats)
                                                   .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5)))
                                                   .Where(d => d.PreferredIDE.In(bestIDEsEver))
                                                   .ToList();
                    Assert.NotEmpty(queryUsingWhereIn);
                }
            }
        }
    }
}
