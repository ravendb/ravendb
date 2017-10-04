// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using System.Linq;
using Xunit;

namespace RavenDB_7942
{
    public class DoubleIndexingTest : RavenTestBase
    {
        [Fact]
        public void DoubleIndexing()
        {
            using (var server = GetNewServer())
            using (var store = NewRemoteDocumentStore(ravenDbServer: server))
            {
                new DocIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Doc { Id = "Docs/1", DoubleValue = 1 });
                    session.Store(new Doc { Id = "Docs/2", DoubleValue = double.NaN });
                    session.Store(new Doc { Id = "Docs/3", DoubleValue = double.PositiveInfinity });
                    session.Store(new Doc { Id = "Docs/4", DoubleValue = double.NegativeInfinity });
                    session.SaveChanges();
                }

                WaitForAllRequestsToComplete(server);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<DocView, DocIndex>()
                        .ProjectFromIndexFieldsInto<DocView>()
                        .ToArray();
                    Assert.Equal(4, results.Length);
                }
            }
        }
    }

    public class DocIndex : AbstractIndexCreationTask<Doc, DocView>
    {
        public DocIndex()
        {
            Map = docs => from doc in docs
                select new
                {
                    Id = doc.Id,
                    DoubleValue = !double.IsNaN((double)(doc.DoubleValue)) && !double.IsInfinity((double)(doc.DoubleValue))
                        ? doc.DoubleValue
                        : (double?)null,
                };
        }
    }

    public class Doc
    {
        public string Id { get; set; }
        public double DoubleValue { get; set; }
    }

    public class DocView
    {
        public string Id { get; set; }
        public double? DoubleValue { get; set; }
    }
}