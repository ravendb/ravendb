// -----------------------------------------------------------------------
//  <copyright file="NimaHa.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.MailingList
{
    public class NimaHa : RavenTestBase
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

                var metadata = new Dictionary<string, object>
                {
                    {Constants.Documents.Metadata.Collection, "Houses" }
                };

                store.Admin.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { "from doc in docs.Houses select new { Rent=doc.ContainsKey(\"Rent\")?doc.Rent:null}" },

                    Name = "HouseByRent"
                }}));

                using (var commands = store.Commands())
                {
                    commands.Put("house/1", null, commands.ParseJson(house1), metadata);
                    commands.Put("house/2", null, commands.ParseJson(house2), metadata);
                    commands.Put("house/3", null, commands.ParseJson(house3), metadata);

                    //Wait for non stale results
                    using (var session = store.OpenSession())
                    {
                        var list = session.Query<dynamic>("HouseByRent").Customize(x => x.WaitForNonStaleResults()).ToList();
                        Assert.Equal(3, list.Count);
                    }

                    var query = commands.Query(new IndexQuery { Query = "FROM INDEX 'HouseByRent' WHERE true AND NOT RENT = null" });

                    Assert.Equal(1, query.TotalResults);
                }
            }
        }
    }
}
