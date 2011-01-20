//-----------------------------------------------------------------------
// <copyright file="ProjectingDocumentId.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
    public class ProjectingDocumentId : LocalClientTest
    {
        [Fact]
        public void WillUseConventionsToSetProjection()
        {
            using(var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Name }",
                                                    Stores = {{"Name", FieldStorage.Yes}}
                                                });

                using(var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Email = "ayende@example.org",
                        Name = "ayende"
                    });

                    s.SaveChanges();
                }

                using(var s = store.OpenSession())
                {
                    var nameAndId = s.Advanced.LuceneQuery<User>("test")
                        .WaitForNonStaleResults()
                        .SelectFields<NameAndId>("Name", "__document_id")
                        .Single();

                    Assert.Equal(nameAndId.Name, "ayende");
                    Assert.Equal(nameAndId.Id, "users/1");
                }
            }       
        }
    }
}
