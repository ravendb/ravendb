using System;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Bugs.Metadata
{
    public class MetadataPropertyInEntity : LocalClientTest
    {
        public class Account
        {
            public string Id { get; set; }
            public long Revision { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void Can_use_entity_property_for_metadata()
        {
            using(var store = NewDocumentStore())
            {
                using(var session = store.OpenSession())
                {
                    var account = new Account
                    {
                        Name = "Hibernating Rhinos"
                    };
                    session.Store(account);
                    session.Advanced.GetMetadataFor(account)["Raven-Document-Revision"] = 1;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.OnDocumentConverted += (entity, document, metadata) =>
                    {
                        if(entity is Account == false)
                            return;
                        ((Account) entity).Revision = metadata.Value<long>("Raven-Document-Revision");
                    };

                    session.Advanced.OnEntityConverted += (entity, document, metadata) =>
                    {
                        if (entity is Account == false)
                            return;
                        document.Remove("Revision");
                    };

                    var account = session.Load<Account>("accounts/1");
                    Assert.Equal(1, account.Revision);
                }
            }
        }

        [Fact]
        public void Can_use_entity_property_for_metadata_with_listener()
        {
            using(var store = NewDocumentStore())
            {
                store.RegisterListener(new RavenDocumentRevisionMetadataToRevisionProperty());
                using(var session = store.OpenSession())
                {
                    var account = new Account
                    {
                        Name = "Hibernating Rhinos"
                    };
                    session.Store(account);
                    session.Advanced.GetMetadataFor(account)["Raven-Document-Revision"] = 1;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var account = session.Load<Account>("accounts/1");
                    account.Name = "Rampaging Rhinos";
                    Assert.Equal(1, account.Revision);
                    session.SaveChanges();
                }

                var jsonDocument = store.DatabaseCommands.Get("accounts/1");
                Assert.Null(jsonDocument.DataAsJson["Revision"]);
            }
        }

        public class RavenDocumentRevisionMetadataToRevisionProperty : IDocumentConversionListener
        {
            /// <summary>
            /// Called when converting an entity to a document and metadata
            /// </summary>
            public void EntityToDocument(object entity, JObject document, JObject metadata)
            {
                if (entity is Account == false)
                    return;
                document.Remove("Revision");
            }

            /// <summary>
            /// Called when converting a document and metadata to an entity
            /// </summary>
            public void DocumentToEntity(object entity, JObject document, JObject metadata)
            {
                if (entity is Account == false)
                    return;
                ((Account)entity).Revision = metadata.Value<long>("Raven-Document-Revision");

            }
        }
    }
}