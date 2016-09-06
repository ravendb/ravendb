// -----------------------------------------------------------------------
//  <copyright file="QueryingOn_A_Prefix.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class QueryingOn_A_Prefix : RavenTestBase
    {
        private class SampleData
        {
            public string Name { get; set; }
        }

        private class SampleData_Index : AbstractIndexCreationTask<SampleData>
        {
            public SampleData_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Name
                              };
                Indexes.Add(x => x.Name, FieldIndexing.Analyzed);
            }
        }

        [Fact]
        public void CanIndexAndQuery()
        {
            using (var store = GetDocumentStore())
            {
                new SampleData_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new SampleData
                    {
                        Name = "Ayende"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SampleData, SampleData_Index>()
                        .Search(a => a.Name, "A*", options: SearchOptions.And, escapeQueryOptions: EscapeQueryOptions.AllowPostfixWildcard)
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .FirstOrDefault();

                    Assert.NotNull(result);
                }
            }
        }
    }
}
