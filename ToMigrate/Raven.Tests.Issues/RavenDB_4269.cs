// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4269.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4269 : RavenTest
    {
        [Fact]
        public void AddingMultipleIndexesShouldSetValidPriority()
        {
            using (var store = NewDocumentStore())
            {
                var definition = new RavenDocumentsByEntityName().CreateIndexDefinition();

                var indexesToAdd = new List<IndexToAdd>();
                indexesToAdd.Add(new IndexToAdd
                {
                    Name = "Index1",
                    Definition = definition,
                    Priority = IndexingPriority.Error
                });

                indexesToAdd.Add(new IndexToAdd
                {
                    Name = "Index2",
                    Definition = definition,
                    Priority = IndexingPriority.Idle
                });

                store.DatabaseCommands.PutIndexes(indexesToAdd.ToArray());

                var i1 = store.DocumentDatabase.IndexStorage.GetIndexInstance("Index1");
                var i2 = store.DocumentDatabase.IndexStorage.GetIndexInstance("Index2");

                Assert.Equal(IndexingPriority.Error, i1.Priority);
                Assert.Equal(IndexingPriority.Idle, i2.Priority);
            }
        }

        [Fact]
        public void AddingMultipleSideBySideIndexesShouldSetValidPriority()
        {
            using (var store = NewDocumentStore())
            {
                var definition = new RavenDocumentsByEntityName().CreateIndexDefinition();

                var indexesToAdd = new List<IndexToAdd>();
                indexesToAdd.Add(new IndexToAdd
                {
                    Name = "Index1",
                    Definition = definition,
                    Priority = IndexingPriority.Error
                });

                indexesToAdd.Add(new IndexToAdd
                {
                    Name = "Index2",
                    Definition = definition,
                    Priority = IndexingPriority.Idle
                });

                store.DatabaseCommands.Admin.StopIndexing();
                store.DatabaseCommands.PutSideBySideIndexes(indexesToAdd.ToArray());

                var i1 = store.DocumentDatabase.IndexStorage.GetIndexInstance("Index1");
                var i2 = store.DocumentDatabase.IndexStorage.GetIndexInstance("Index2");

                Assert.Equal(IndexingPriority.Error, i1.Priority);
                Assert.Equal(IndexingPriority.Idle, i2.Priority);
            }
        }
    }
}