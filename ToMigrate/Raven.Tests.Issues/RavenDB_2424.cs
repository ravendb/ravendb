// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2424.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
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
                var initialIndexDef = "from doc in docs select new { doc.Date}";
                Assert.True(store.DatabaseCommands.IndexHasChanged("Index1", new IndexDefinition
                                                                 {
                                                                     Map = initialIndexDef
                                                                 }));

                store.DatabaseCommands.PutIndex("Index1",
                                                new IndexDefinition
                                                {
                                                    Map = initialIndexDef
                                                });

                Assert.False(store.DatabaseCommands.IndexHasChanged("Index1", new IndexDefinition
                {
                    Map = initialIndexDef
                }));

                Assert.True(store.DatabaseCommands.IndexHasChanged("Index1", new IndexDefinition
                {
                    Map = "from doc1 in docs select new { doc1.Date }"
                }));


            }
        }
    }
}
