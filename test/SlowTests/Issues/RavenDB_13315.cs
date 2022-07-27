using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13315:RavenTestBase
    {
        public RavenDB_13315(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SaveChangesShouldNotCreateUnneccessaryAllocations()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    User user = new User
                    {
                        Name = "Jennifer",
                        Id = "users/1"
                    };
                    session.Store(user);
                    session.SaveChanges();

                    var context = (session as InMemoryDocumentSessionOperations).Context;

                    var allocatedMemoryBeforeOperationsThatShouldHaveNoMemoryFootpring = context.AllocatedMemory;
                    session.SaveChanges();
                    Assert.False((session as InMemoryDocumentSessionOperations).HasChanges);
                    Assert.False((session as InMemoryDocumentSessionOperations).HasChanged(user));
                    session.Delete("users/1", null);
                    session.SaveChanges();
                    Assert.True(allocatedMemoryBeforeOperationsThatShouldHaveNoMemoryFootpring >= context.AllocatedMemory, $"{allocatedMemoryBeforeOperationsThatShouldHaveNoMemoryFootpring} >= {context.AllocatedMemory}");
                }
            }
        }

    }
}
