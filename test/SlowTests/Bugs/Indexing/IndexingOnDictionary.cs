//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using Xunit;
using System.Linq;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Indexing
{
    public class IndexingOnDictionary : RavenTestBase
    {
        public IndexingOnDictionary(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanIndexValuesForDictionary(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Items = new Dictionary<string, string>
                                                {
                                                    {"Color", "Red"}
                                                }
                    });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var users = s.Advanced.DocumentQuery<User>()
                        .WhereEquals("Items.Color", "Red")
                        .ToArray();
                    Assert.NotEmpty(users);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanIndexValuesForDictionaryAsPartOfDictionary(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Items = new Dictionary<string, string>
                                                {
                                                    {"Color", "Red"}
                                                }
                    });
                    
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var users = s.Advanced.DocumentQuery<User>()
                        .WhereEquals("Items[].Key", "Color")
                        .AndAlso()
                        .WhereEquals("Items[].Value", "Red")
                        .ToArray();

                    Assert.NotEmpty(users);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanIndexNestedValuesForDictionaryAsPartOfDictionary(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        NestedItems = new Dictionary<string, NestedItem>
                        {
                            { "Color", new NestedItem{ Name="Red" } }
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var users = s.Advanced.DocumentQuery<User>()
                        .WhereEquals("NestedItems[].Key", "Color")
                        .AndAlso()
                        .WhereEquals("NestedItems[].Name", "Red")
                        .ToArray();
                    Assert.NotEmpty(users);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanIndexValuesForIDictionaryAsPartOfIDictionary(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new UserWithIDictionary
                    {
                        Items = new Dictionary<string, string>
                            {
                                { "Color", "Red" }
                            }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var users = s.Advanced.DocumentQuery<UserWithIDictionary>()
                        .WhereEquals("Items[].Key", "Color")
                        .AndAlso()
                        .WhereEquals("Items[].Value", "Red")
                        .ToArray();
                    Assert.NotEmpty(users);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanIndexNestedValuesForIDictionaryAsPartOfIDictionary(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new UserWithIDictionary
                    {
                        NestedItems = new Dictionary<string, NestedItem>
                        {
                            { "Color", new NestedItem{ Name="Red" } }
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var users = s.Advanced.DocumentQuery<UserWithIDictionary>()
                        .WhereEquals("NestedItems[].Key", "Color")
                        .AndAlso()
                        .WhereEquals("NestedItems[].Name", "Red")
                        .ToArray();
                    Assert.NotEmpty(users);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanIndexValuesForDictionaryWithNumberForIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Items = new Dictionary<string, string>
                                                {
                                                    {"3", "Red"}
                                                }
                    });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var users = s.Advanced.DocumentQuery<User>()
                        .WhereEquals("Items[].3", "Red")
                        .ToArray();
                    Assert.NotEmpty(users);
                }
            }
        }

        #region Nested type: User / UserWithIDictionary / NestedItem

        private class User
        {
            public string Id { get; set; }
            public Dictionary<string, string> Items { get; set; }
            public Dictionary<string, NestedItem> NestedItems { get; set; }
        }

        private class UserWithIDictionary
        {
            public string Id { get; set; }
            public IDictionary<string, string> Items { get; set; }
            public IDictionary<string, NestedItem> NestedItems { get; set; }
        }

        private class NestedItem { public string Name { get; set; } }

        #endregion
    }
}
