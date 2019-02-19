// -----------------------------------------------------------------------
//  <copyright file="RavenDB1019.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB1019 : RavenTestBase
    {
        [Fact]
        public void StreamDocsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "Test" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var enumerator = session.Advanced.Stream<object>(startsWith:"");

                    var count = 0;
                    while (enumerator.MoveNext())
                    {
                        count++;
                    }

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public void CanDisposeEarly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        session.Store(new User() { Name = "Test" });
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var enumerator = session.Advanced.Stream<object>(startsWith:"");

                    if (enumerator.MoveNext())
                        enumerator.Dispose();
                }
            }
        }
    }
}
