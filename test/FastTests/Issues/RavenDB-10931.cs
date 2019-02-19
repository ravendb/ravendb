using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10931 : RavenTestBase
    {
        public class Foo
        {
            public string Bar { get; set; }
        }

        [Fact]
        public void Subscribing_to_OnBeforeStore_event_should_not_prevent_metadata_to_be_saved()
        {
            using(var server = GetNewServer())
            using(var store = new DocumentStore
            {
                Database = "Test",
                Urls = new[] { server.WebUrl },
                Conventions =
                    {
                        IdentityPartsSeparator = "-"
                    }
            })
            {
                store.OnBeforeStore += (s, a) => { };
                store.Initialize();

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord("Test")));

                var verificationValue = Guid.NewGuid().ToString();
                using (var session = store.OpenSession())
                {
                    var baz = new Foo
                    {
                        Bar = "wibble"
                    };

                    session.Store(baz);
                    session.Advanced.GetMetadataFor(baz)["Info"] = verificationValue;

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var baz = session.Query<Foo>().First();

                    Assert.Equal("wibble", baz.Bar); //sanity check
                    Assert.Equal(verificationValue, session.Advanced.GetMetadataFor(baz)["Info"]);
                }
            }
        }
    }
}
