using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class RemoteIndexingOnDictionary : RavenTestBase
    {
        [Fact]
        public void CanIndexOnRangeForNestedValuesForDictionaryAsPartOfDictionary()
        {
                DoNotReuseServer();            
                var name = "RemoteIndexingOnDictionary_1";
                var doc = MultiDatabase.CreateDatabaseDocument(name);

                using (var store = new DocumentStore { Url = UseFiddler(Server.WebUrls[0]), DefaultDatabase = name }.Initialize())
                {
                    store.DatabaseCommands.GlobalAdmin.CreateDatabase(doc);

                    using (var s = store.OpenSession())
                    {
                        s.Store(new UserWithIDictionary
                        {
                            NestedItems = new Dictionary<string, NestedItem>
                            {
                                { "Color", new NestedItem{ Value=50 } }
                            }
                        });
                        s.SaveChanges();
                    }

                    using (var s = store.OpenSession())
                    {
                        s.Advanced.DocumentQuery<UserWithIDictionary>()
                         .WhereEquals("NestedItems,Key", "Color")
                         .AndAlso()
                         .WhereGreaterThan("NestedItems,Value.Value", 10)
                         .ToArray();
                    }
                }
            
        }

        #region Nested type: UserWithIDictionary / NestedItem
        private class UserWithIDictionary
        {
            public string Id { get; set; }
            public IDictionary<string, string> Items { get; set; }
            public IDictionary<string, NestedItem> NestedItems { get; set; }
        }

        private class NestedItem
        {
            public string Name { get; set; }
            public double Value { get; set; }
        }

        #endregion
    }
}