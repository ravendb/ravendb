using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14276 : RavenTestBase
    {
        public RavenDB_14276(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Update_Metadata_With_Nested_Dictionary()
        {
            using (IDocumentStore store = GetDocumentStore())
            {
                store.OnBeforeStore += OnBeforeStore;
                const string docId = "users/1";

                using (var session = store.OpenSession())
                {
                    var user = new User
                    {
                        Name = "Some document"
                    };
                    session.Store(user, docId);

                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata["Custom-Metadata"] = _dictionary;

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(docId);
                    user.Name = "Updated document";

                    session.SaveChanges();
                }

                VerifyData(store, docId);
            }
        }

        [Fact]
        public void Can_Update_Metadata_With_Nested_Dictionary_Same_Session()
        {
            using (IDocumentStore store = GetDocumentStore())
            {
                store.OnBeforeStore += OnBeforeStore;
                const string docId = "users/1";

                using (var session = store.OpenSession())
                {
                    var savedUser = new User
                    {
                        Name = "Some document"
                    };
                    session.Store(savedUser, docId);

                    var metadata = session.Advanced.GetMetadataFor(savedUser);
                    metadata["Custom-Metadata"] = _dictionary;

                    session.SaveChanges();

                    var user = session.Load<User>(docId);
                    user.Name = "Updated document";
                    session.SaveChanges();
                }

                VerifyData(store, docId);
            }
        }

        private static void OnBeforeStore(object sender, BeforeStoreEventArgs eventArgs)
        {
            if (eventArgs.DocumentMetadata.ContainsKey("Some-MetadataEntry"))
            {
                var metadata = eventArgs.Session.GetMetadataFor(eventArgs.Entity);
                metadata["Some-MetadataEntry"] = "Updated";
            }
            else
            {
                eventArgs.DocumentMetadata.Add("Some-MetadataEntry", "Created");
            }
        }

        private static void VerifyData(IDocumentStore store, string docId)
        {
            using (var session = store.OpenSession())
            {
                var user = session.Load<User>(docId);
                Assert.Equal("Updated document", user.Name);

                var metadata = session.Advanced.GetMetadataFor(user);
                var dictionary = metadata.GetObject("Custom-Metadata");
                var nestedDictionary = dictionary.GetObject("123");
                Assert.Equal(1, nestedDictionary.GetLong("aaaa"));

                nestedDictionary = dictionary.GetObject("321");
                Assert.Equal(2, nestedDictionary.GetLong("bbbb"));
            }
        }

        private readonly Dictionary<string, Dictionary<string, int>> _dictionary = new Dictionary<string, Dictionary<string, int>>
        {
            {
                "123", new Dictionary<string, int> { { "aaaa", 1 } }
            },
            {
                "321", new Dictionary<string, int> { { "bbbb", 2 } }
            }
        };
    }
}
