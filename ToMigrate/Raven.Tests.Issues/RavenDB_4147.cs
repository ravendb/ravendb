// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4147.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    
    public class RavenDB_4147 : RavenTest
    {

        public class Dates
        {
            public string Date1 { get; set; }
            public string Date2 { get; set; }
        }


        [Fact]
        public void LastModifiedShouldBeAvailableInPatchContext()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dates());
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                store.DatabaseCommands.Patch("dates/1", new ScriptedPatchRequest
                {
                    Script = "this.Date1 = this[\"@metadata\"][\"Last-Modified\"]; this.Date2 = this[\"@metadata\"][\"Raven-Last-Modified\"];"
                });

                using (var session = store.OpenSession())
                {
                    var dates = session.Load<Dates>("dates/1");
                    Assert.NotNull(dates.Date1);
                    Assert.NotNull(dates.Date2);
                }
            }
        }
    }
}
