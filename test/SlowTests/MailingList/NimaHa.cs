// -----------------------------------------------------------------------
//  <copyright file="NimaHa.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class NimaHa : RavenNewTestBase
    {
        [Fact]
        public void NullValueTest()
        {
            using (var store = GetDocumentStore())
            {
                const string house1 = @"{
                    'Rent' : 1200
                }";

                const string house2 = @"{
                                'Rent' : null
                                }";

                const string house3 = @"{
                                }";

                var metadata = new Dictionary<string, string>
                {
                    {Constants.Metadata.Collection, "Houses" }
                };

                store.Admin.Send(new PutIndexOperation("HouseByRent", new IndexDefinition
                {
                    Maps = { "from doc in docs.Houses select new { Rent=doc.ContainsKey(\"Rent\")?doc.Rent:null}" },

                    Name = "HouseByRent"
                }));

                using (var commands = store.Commands())
                {
                    commands.Put("house/1", 0, commands.ParseJson(house1), metadata);
                    commands.Put("house/2", 0, commands.ParseJson(house2), metadata);
                    commands.Put("house/3", 0, commands.ParseJson(house3), metadata);

                    //Wait for non stale results
                    using (var session = store.OpenSession())
                    {
                        var list = session.Query<dynamic>("HouseByRent").Customize(x => x.WaitForNonStaleResults()).ToList();
                        Assert.Equal(3, list.Count);
                    }

                    var query = commands.Query("HouseByRent", new IndexQuery(store.Conventions) { Query = "*:* AND -Rent:[[NULL_VALUE]]" });

                    Assert.Equal(1, query.TotalResults);
                }
            }
        }
    }
}
