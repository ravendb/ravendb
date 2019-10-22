// -----------------------------------------------------------------------
//  <copyright file="QueryingOn_A_Prefix.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class QueryingOn_A_Prefix : RavenTestBase
    {
        public QueryingOn_A_Prefix(ITestOutputHelper output) : base(output)
        {
        }

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
                Indexes.Add(x => x.Name, FieldIndexing.Search);
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
                        .Search(a => a.Name, "A*", options: SearchOptions.And)
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .FirstOrDefault();

                    Assert.NotNull(result);
                }
            }
        }
    }
}
