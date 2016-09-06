using System;
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class LastModifiedMetadataTest : RavenTestBase
    {
        private class AmazingIndex2 : AbstractIndexCreationTask<User>
        {
            public AmazingIndex2()
            {
                Map = docs =>
                      from doc in docs
                      select new
                      {
                          LastModified = MetadataFor(doc)["Last-Modified"],
                      };
            }
        }

        private class AmazingTransformer2 : AbstractTransformerCreationTask<User>
        {
            public class ModifiedDocuments
            {
                public string InternalId { get; set; }
                public DateTime LastModified { get; set; }
            }

            public AmazingTransformer2()
            {
                TransformResults = results => from doc in results
                                              select new
                                              {
                                                  InternalId = MetadataFor(doc)["@id"],
                                                  LastModified = MetadataFor(doc)["Last-Modified"],
                                              };
            }
        }

        private class User
        {
            public string InternalId { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void Can_index_and_query_metadata2()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.FindIdentityProperty = info => info.Name == "InternalId";

                var user1 = new User { Name = "Joe Schmoe" };
                var user2 = new User { Name = "Jack Spratt" };
                new AmazingIndex2().Execute(store);
                new AmazingTransformer2().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(user1);
                    session.Store(user2);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var user3 = session.Load<User>(user1.InternalId);
                    Assert.NotNull(user3);
                    var metadata = session.Advanced.GetMetadataFor(user3);
                    var lastModified = metadata.Value<DateTime>("Last-Modified");

                    var modifiedDocuments = (from u in session.Query<User, AmazingIndex2>()
                                                .TransformWith<AmazingTransformer2, AmazingTransformer2.ModifiedDocuments>()
                                                 .Customize(x => x.WaitForNonStaleResults())
                                             orderby u.InternalId
                                             select u).ToList();

                    Assert.Equal(2, modifiedDocuments.Count);
                    Assert.Equal(user1.InternalId, modifiedDocuments[0].InternalId);

                    Assert.Equal(lastModified.ToString("yyyy-MM-dd hh:mm:ss"), modifiedDocuments[0].LastModified.ToString("yyyy-MM-dd hh:mm:ss"));

                }
            }
        }
    }
}
