//-----------------------------------------------------------------------
// <copyright file="TransitiveNull.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
    public class TransitiveNull : LocalClientTest
    {
        [Fact]
        public void WillNotError()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("users/1", null,
                                           new JObject
                                           {
                                               {"Name", "user1"},
                                           },
                                           new JObject());

                store.DatabaseCommands.Put("users/2", null,
                                           new JObject
                                           {
                                               {"Name", "user2"},
                                           },
                                           new JObject());

                store.DatabaseCommands.Query("dynamic", new IndexQuery
                {
                    Query = "Tags,:abc"
                }, new string[0]);

                Assert.Empty(store.DocumentDatabase.Statistics.Errors);
            }
        }

        [Fact]
        public void WillNotIncludeDocumentsThatHasNoItemsToIndex()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("users/1", null,
                                           new JObject
                                           {
                                               {"Name", "user1"},
                                           },
                                           new JObject());

                store.DatabaseCommands.Put("users/2", null,
                                           new JObject
                                           {
                                               {"Username", "user2"},
                                           },
                                           new JObject());

                store.DatabaseCommands.Query("dynamic", new IndexQuery
                {
                    Query = "Name:abc"
                }, new string[0]);


                var tempIndex = store.DocumentDatabase.IndexStorage.Indexes.First(x=>x.StartsWith("Temp"));
                var results = store.OpenSession().Advanced.LuceneQuery<dynamic>(tempIndex).WaitForNonStaleResults().ToArray();

                Assert.Equal(1, results.Length);
            }
        }
    }
}