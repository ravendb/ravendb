using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12780 : RavenTestBase
    {
        [Fact]
        public void Can_access_id_of_a_missing_loaded_document()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { @"from doc in docs.Users
                        select new{
                            Id1 = Id(LoadDocument(""users/2"", ""users"")),
                            Id2 = Id(doc.AddressId).Name
                        }" },
                    Name = "IdIndex"
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "IdIndex" }));
                Assert.Empty(stats[0].Errors);
            }
        }

        [Fact]
        public void Can_access_metadata_of_a_missing_loaded_document()
        {
            using (var store = GetDocumentStore())
            {
                new MetadataIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "MetadataIndex" }));
                Assert.Empty(stats[0].Errors);
            }
        }

        [Fact]
        public void Can_access_asjson_of_a_missing_loaded_document()
        {
            using (var store = GetDocumentStore())
            {
                new AsJsonIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "AsJsonIndex" }));
                Assert.Empty(stats[0].Errors);
            }
        }

        private class MetadataIndex : AbstractIndexCreationTask<User>
        {
            public MetadataIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        Metadata1 = MetadataFor(LoadDocument<Address>(user.AddressId)).Value<string>("Name"),
                        Metadata2 = MetadataFor(user.AddressId).Value<string>("Name"),
                        Attachments1 = AttachmentsFor(LoadDocument<Address>(user.AddressId)).Count(),
                        Attachments2 = AttachmentsFor(user.AddressId).Count(),
                        Counters1 = CounterNamesFor(LoadDocument<Address>(user.AddressId)),
                        Counters2 = CounterNamesFor(user.AddressId)
                    };
            }
        }

        private class AsJsonIndex : AbstractIndexCreationTask<User>
        {
            public AsJsonIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        AsJson1 = AsJson(LoadDocument<Address>(user.AddressId)).Value<string>("Name"),
                        AsJson2 = AsJson(user.AddressId).Value<string>("Name"),
                    };
            }
        }
    }
}
