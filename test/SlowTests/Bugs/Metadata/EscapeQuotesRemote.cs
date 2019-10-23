//-----------------------------------------------------------------------
// <copyright file="LastModifiedRemote.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Metadata
{
    public class EscapeQuotesRemote : RavenTestBase
    {
        public EscapeQuotesRemote(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanProperlyEscapeQuotesInMetadata_Remote_1()
        {
            var name = "CanProperlyEscapeQuotesInMetadata_Remote_1";
            var doc = new DatabaseRecord(name);

            DoNotReuseServer();
            using (var store = new DocumentStore { Urls = UseFiddler(Server.WebUrl), Database = name }.Initialize())
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

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
        public void CanProperlyEscapeQuotesInMetadata_Remote_2()
        {
            var name = "CanProperlyEscapeQuotesInMetadata_Remote_2";
            var doc = new DatabaseRecord(name);

            DoNotReuseServer();
            using (var store = new DocumentStore { Urls = UseFiddler(Server.WebUrl), Database = name }.Initialize())
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

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
