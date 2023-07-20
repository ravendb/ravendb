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

                AssertRequestCountEqual(metadataFor =>
                {
                    foreach (var keyValue in metadataFor)
                    {

                    }
                });
                AssertRequestCountEqual(metadataFor => metadataFor["@collection"] = "Users");
                AssertRequestCountEqual(metadataFor => metadataFor.Remove(Constants.Documents.Metadata.Refresh));
                AssertRequestCountEqual(metadataFor => metadataFor.Remove(new KeyValuePair<string, object>(Constants.Documents.Metadata.Refresh, "122123")));
                AssertRequestCountEqual(metadataFor => metadataFor.CopyTo(new KeyValuePair<string, object>[metadataFor.Count], 0));

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
        public void Metadata_that_changed_caused_saveChanges()
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

                AssertRequestCount(metadataFor => metadataFor.Remove(Constants.Documents.Metadata.LastModified), requestCountEqual: true);
                AssertRequestCount(metadataFor => metadataFor.Remove(new KeyValuePair<string, object>(Constants.Documents.Metadata.LastModified, metadataFor[Constants.Documents.Metadata.LastModified])), requestCountEqual: true);
                AssertRequestCount(metadataFor => metadataFor["@last-modified"] = "Users", requestCountEqual: true);
                AssertRequestCount(metadataFor =>
                {
                    metadataFor["@last-modified"] = null;
                    metadataFor["@last-modified"] = "Users";
                }, requestCountEqual: true);
                AssertRequestCount(metadataFor => metadataFor["@1234"] = "Users");
                AssertRequestCount(metadataFor => metadataFor.Add(new KeyValuePair<string, object>("@sfddfdsf", "fgffffg")));
                AssertRequestCount(metadataFor => metadataFor.Add("@ewewrewr", "fdsfdgfdg"));

                void AssertRequestCount(Action<IMetadataDictionary> action, bool requestCountEqual = false)
                {
                    using (var session = store.OpenSession())
                    {
                        var entity = session.Load<User>(id);
                        var metadataFor = session.Advanced.GetMetadataFor(entity);

                        var requestsBefore = session.Advanced.NumberOfRequests;

                        action(metadataFor);

                        session.SaveChanges();

                        if (requestCountEqual == false)
                            Assert.NotEqual(requestsBefore, session.Advanced.NumberOfRequests);
                        else
                            Assert.Equal(requestsBefore, session.Advanced.NumberOfRequests);
                    }
                }
            }
        }

        [Fact]
        public void Metadata_clear_caused_saveChanges()
        {
            using (var store = GetDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user);
                    id = user.Id;

                    var metadataFor = session.Advanced.GetMetadataFor(user);
                    metadataFor["test"] = "1";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entity = session.Load<User>(id);
                    var metadataFor = session.Advanced.GetMetadataFor(entity);

                    var requestsBefore = session.Advanced.NumberOfRequests;

                    metadataFor.Clear();

                    session.SaveChanges();

                    Assert.NotEqual(requestsBefore, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.False(metadata.Keys.Contains("test"));
                }
            }
        }

        [Fact]
        public void Metadata_clear_that_didnt_change_doesnt_cause_saveChanges()
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

                using (var session = store.OpenSession())
                {
                    var entity = session.Load<User>(id);
                    var metadataFor = session.Advanced.GetMetadataFor(entity);

                    var requestsBefore = session.Advanced.NumberOfRequests;

                    metadataFor.Clear();

                    session.SaveChanges();

                    Assert.Equal(requestsBefore, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
