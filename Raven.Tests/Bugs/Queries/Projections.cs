//-----------------------------------------------------------------------
// <copyright file="Projections.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Database.Data;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
    public class Projections : LocalClientTest
    {
        [Fact]
        public void Can_project_value_from_collection()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Addresses = new[]
                        {
                            new LiveProjection.Address
                            {
                                Name = "Hadera"
                            },
                            new LiveProjection.Address
                            {
                                Name = "Tel Aviv"
                            },
               
                        }
                    });
                    s.SaveChanges();
                }

                var queryResult = store.DatabaseCommands.Query("dynamic",
                                                               new IndexQuery
                                                               {
                                                                   FieldsToFetch = new[] { "Addresses,Name" }
                                                               }, 
                                                               new string[0]);

                Assert.Equal(2, queryResult.Results[0]["Addresses,Name"].Count());
            }
        }

        public class User
        {
            public LiveProjection.Address[] Addresses { get; set; }
        }
    }
}