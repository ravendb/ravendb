using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17996 : RavenTestBase
    {
        public RavenDB_17996(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Metadata_that_didnt_change_doesnt_cause_saveChanges()
        {
            using (var store = GetDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user);
                    id = user.Id;
                    session.SaveChanges();
                }
                
                GetValues(id, store);
                AssertRequestCountEqual(metadataFor => metadataFor["@collection"] = "Users");
                AssertRequestCountEqual(metadataFor => metadataFor.Remove(Constants.Documents.Metadata.Refresh));
                AssertRequestCountEqual(metadataFor => metadataFor.Remove(new KeyValuePair<string, object>(Constants.Documents.Metadata.Refresh, "122123")));
                AssertRequestCountEqual(metadataFor => metadataFor.CopyTo(new System.Collections.Generic.KeyValuePair<string, object>[metadataFor.Count], 0));

                void AssertRequestCountEqual(Action<IMetadataDictionary> action)
                {
                    using (var session = store.OpenSession())
                    {
                        var entity = session.Load<User>(id);
                        var metadataFor = session.Advanced.GetMetadataFor(entity);

                        var requestsBefore = session.Advanced.NumberOfRequests;

                        action(metadataFor);

                        session.SaveChanges();

                        Assert.Equal(requestsBefore, session.Advanced.NumberOfRequests);
                    }
                }
            }
        }

        [Fact]
        public void Metadata_that_changed_cause_saveChanges()
        {
            using (var store = GetDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user);
                    id = user.Id;
                    session.SaveChanges();
                }

                AssertRequestCountNotEqual(metadataFor => metadataFor.Remove(Constants.Documents.Metadata.LastModified));
                AssertRequestCountNotEqual(metadataFor => metadataFor.Remove(new KeyValuePair<string, object>(Constants.Documents.Metadata.LastModified, metadataFor[Constants.Documents.Metadata.LastModified])));
                AssertRequestCountNotEqual(metadataFor => metadataFor["@last-modified"] = "Users");
                AssertRequestCountNotEqual(metadataFor => metadataFor["@1234"] = "Users");
                AssertRequestCountNotEqual(metadataFor => metadataFor.Add(new KeyValuePair<string, object>("@sfddfdsf", "fgffffg")));
                AssertRequestCountNotEqual(metadataFor => metadataFor.Add("@ewewrewr", "fdsfdgfdg"));

                void AssertRequestCountNotEqual(Action<IMetadataDictionary> action)
                {
                    using (var session = store.OpenSession())
                    {
                        var entity = session.Load<User>(id);
                        var metadataFor = session.Advanced.GetMetadataFor(entity);

                        var requestsBefore = session.Advanced.NumberOfRequests;

                        action(metadataFor);

                        session.SaveChanges();

                        Assert.NotEqual(requestsBefore, session.Advanced.NumberOfRequests);
                    }
                }
            }
        }
        private void GetValues(string id, DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var entity = session.Load<User>(id);
                var metadataFor = session.Advanced.GetMetadataFor(entity);

                var requestsBefore = session.Advanced.NumberOfRequests;

                foreach (var keyValue in metadataFor)
                {

                }

                session.SaveChanges();

                Assert.Equal(requestsBefore, session.Advanced.NumberOfRequests);
            }
            
        }
    }
}
