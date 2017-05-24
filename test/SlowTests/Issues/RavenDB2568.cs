// -----------------------------------------------------------------------
//  <copyright file="RavenDB2568.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB2568 : RavenTestBase
    {
        [Fact]
        public void SimpleSkipAfter()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("users/01", null, new { }, null);
                    commands.Put("users/02", null, new { }, null);
                    commands.Put("users/03", null, new { }, null);
                    commands.Put("users/10", null, new { }, null);
                    commands.Put("users/12", null, new { }, null);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.LoadStartingWith<object>("users/", startAfter: "users/02");
                    Assert.Equal(3, results.Length);
                    Assert.Equal("users/03", session.Advanced.GetDocumentId(results[0]));
                    Assert.Equal("users/10", session.Advanced.GetDocumentId(results[1]));
                    Assert.Equal("users/12", session.Advanced.GetDocumentId(results[2]));
                }
            }
        }

        [Fact]
        public void StreamingSkipAfter()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("users/01", null, new { }, null);
                    commands.Put("users/02", null, new { }, null);
                    commands.Put("users/03", null, new { }, null);
                    commands.Put("users/10", null, new { }, null);
                    commands.Put("users/12", null, new { }, null);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.Stream<dynamic>(startsWith: "users/", startAfter: "users/02");

                    Assert.True(results.MoveNext());
                    Assert.Equal("users/03", results.Current.Id);
                    Assert.True(results.MoveNext());
                    Assert.Equal("users/10", results.Current.Id);
                    Assert.True(results.MoveNext());
                    Assert.Equal("users/12", results.Current.Id);
                    Assert.False(results.MoveNext());
                }
            }
        }
    }
}
