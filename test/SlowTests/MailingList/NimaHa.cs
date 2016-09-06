// -----------------------------------------------------------------------
//  <copyright file="NimaHa.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Json.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class NimaHa : RavenTestBase
    {
        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void NullValueTest()
        {
            using (var store = GetDocumentStore())
            {
                const string house1 = @"{
                    Rent : 1200
                }";

                const string house2 = @"{
                                Rent : null
                                }";

                const string house3 = @"{
                                }";

                const string metadata = @"{ 
                                    ""Raven-Entity-Name"" : ""Houses"" 
                                 }";

                store.DatabaseCommands.PutIndex("HouseByRent", new IndexDefinition
                {
                    Maps = { "from doc in docs.Houses select new { Rent=doc.Inner.ContainsKey(\"Rent\")?doc.Rent:null}" },

                    Name = "HouseByRent"
                });

                store.DatabaseCommands.Put("house/1", 0, RavenJObject.Parse(house1), RavenJObject.Parse(metadata));
                store.DatabaseCommands.Put("house/2", 0, RavenJObject.Parse(house2), RavenJObject.Parse(metadata));
                store.DatabaseCommands.Put("house/3", 0, RavenJObject.Parse(house3), RavenJObject.Parse(metadata));

                //Wait for non stale results
                using (var session = store.OpenSession())
                {
                    var list = session.Query<dynamic>("HouseByRent").Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Equal(3, list.Count);
                }

                var query = store.DatabaseCommands.Query("HouseByRent", new IndexQuery { Query = "*:* AND -Rent:[[NULL_VALUE]]" });

                Assert.Equal(1, query.TotalResults);
            }
        }
    }
}
