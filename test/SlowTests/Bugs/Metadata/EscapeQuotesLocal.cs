//-----------------------------------------------------------------------
// <copyright file="LastModifiedRemote.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Metadata
{
    public class EscapeQuotesLocal : RavenTestBase
    {
        public EscapeQuotesLocal(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanProperlyEscapeQuotesInMetadata_Local_1()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user);
                    session.Advanced.GetMetadataFor(user).Add("Foo", "\"Bar\"");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal("\"Bar\"", metadata["Foo"]);
                }
            }
        }

        [Fact]
        public void CanProperlyEscapeQuotesInMetadata_Local_2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user);
                    session.Advanced.GetMetadataFor(user).Add("Foo", "\\\"Bar\\\"");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal("\\\"Bar\\\"", metadata["Foo"]);
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
