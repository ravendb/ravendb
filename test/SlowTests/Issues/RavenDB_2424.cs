// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2424.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2424 : RavenTestBase
    {
        public RavenDB_2424(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void HasChangedWorkProperly(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const string initialIndexDef = "from doc in docs select new { doc.Date}";
                Assert.True(store.Maintenance.Send(new IndexHasChangedOperation(new IndexDefinition
                {
                    Name = "Index1",
                    Maps = { initialIndexDef }
                })));

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Index1",
                    Maps = { initialIndexDef }
                }));

                Assert.False(store.Maintenance.Send(new IndexHasChangedOperation(new IndexDefinition
                {
                    Name = "Index1",
                    Maps = { initialIndexDef }
                })));

                Assert.True(store.Maintenance.Send(new IndexHasChangedOperation(new IndexDefinition
                {
                    Name = "Index1",
                    Maps = { "from doc1 in docs select new { doc1.Date }" }
                })));
            }
        }
    }
}
