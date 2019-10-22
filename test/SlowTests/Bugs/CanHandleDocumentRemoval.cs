//-----------------------------------------------------------------------
// <copyright file="CanHandleDocumentRemoval.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using FastTests;
using Xunit;
using System.Linq;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CanHandleDocumentRemoval : RavenTestBase
    {
        public CanHandleDocumentRemoval(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanHandleDocumentDeletion()
        {
            using(var store = GetDocumentStore())
            {
                using(var session = store.OpenSession())
                {
                    for (int i = 0; i < 3; i++)
                    {
                        session.Store(new User
                        {
                            Name = "ayende"
                        });
                    }
                    session.SaveChanges();
                }
         
                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToArray();
                    Assert.NotEmpty(users);
                    foreach (var user in users)
                    {
                        session.Delete(user);
                    }
                    session.SaveChanges();
                }
           
                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5)))
                        .ToArray();
                    Assert.Empty(users);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }

    }
}
