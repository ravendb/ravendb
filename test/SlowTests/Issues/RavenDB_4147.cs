// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4147.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4147 : RavenTestBase
    {
        public class Dates
        {
            public string Date1 { get; set; }
            public string Date2 { get; set; }
        }

        [Fact]
        public void LastModifiedShouldBeAvailableInPatchContext()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dates());
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                store.Operations.Send(new PatchOperation("dates/1-A", null, new PatchRequest
                {
                    Script = "this.Date1 = this[\"@metadata\"][\"@last-modified\"]; this.Date2 = this[\"@metadata\"][\"@last-modified\"];"
                }));

                using (var session = store.OpenSession())
                {
                    var dates = session.Load<Dates>("dates/1-A");
                    Assert.NotNull(dates.Date1);
                    Assert.NotNull(dates.Date2);
                }
            }
        }
    }
}
