//-----------------------------------------------------------------------
// <copyright file="ProjectingDocumentId.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
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


        public class MyEntity
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
        }

        public void ShouldIdentifyDocumentIdAlsoWithProjectionRetrieveDocumentInternal()
        {
            using (var store = NewDocumentStore())
            {
                Guid id = Guid.NewGuid();
                using (var s = store.OpenSession())
                {
                    s.Store(new MyEntity { Id = id, Name = "test", Description = "my test" });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<MyEntity>();
                    var l1 = results.ToList();
                    Assert.Equal(id, l1[0].Id);

                    var l2 = results.Select(x => new { x.Id, x.Name, x.Description }).ToList();
                    Assert.Equal(id, l2[0].Id);

                    // try partial pull
                    var l3 = results.Select(x => new { x.Id, x.Name, }).ToList();
                    Assert.Equal(id, l3[0].Id);
                }
            }
        }
    }
}
