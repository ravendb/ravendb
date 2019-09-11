// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2424.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Indexing;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2424 : RavenTest
    {
        [Fact]
        public void HasChangedWorkProperly()
        {
            using (var store = NewDocumentStore())
            {
                const string initialIndexDef = "from doc in docs select new { doc.Date}";
                const string indexName = "Index1";
                Assert.True(store.DatabaseCommands.IndexHasChanged(indexName, new IndexDefinition
                                                                 {
                                                                     Map = initialIndexDef
                                                                 }));

                store.DatabaseCommands.PutIndex("Index1",
                                                new IndexDefinition
                                                {
                                                    Map = initialIndexDef
                                                });

                Assert.False(store.DatabaseCommands.IndexHasChanged(indexName, new IndexDefinition
                {
                    Map = initialIndexDef
                }));

                Assert.True(store.DatabaseCommands.IndexHasChanged(indexName, new IndexDefinition
                {
                    Map = "from doc1 in docs select new { doc1.Date }"
                }));


            }
        }

        [Fact]
        public void HasChangedWorkProperly2()
        {
            using (var store = NewDocumentStore())
            {
                const string initialIndexDef = "from doc in docs select new { doc.Date}";
                const string indexName = "Index1";
                Assert.True(store.DatabaseCommands.IndexHasChanged(indexName, new IndexDefinition
                {
                    Map = initialIndexDef
                }));

                store.DatabaseCommands.PutIndex("Index1",
                    new IndexDefinition
                    {
                        Map = initialIndexDef
                    });

                Assert.False(store.DatabaseCommands.IndexHasChanged(indexName, new IndexDefinition
                {
                    Map = initialIndexDef
                }));

                Assert.False(store.DatabaseCommands.IndexHasChanged(indexName, new IndexDefinition
                {
                    Map = "from doc in docs select new {      doc.Date      }"
                }));
            }
        }
    }
}
