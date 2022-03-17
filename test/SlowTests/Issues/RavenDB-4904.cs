// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4904 : RavenTestBase
    {
        public RavenDB_4904(ITestOutputHelper output) : base(output)
        {
        }

        private const string IndexName = "testIndex";

        [Fact(Skip = "RavenDB-5919")]
        public void can_create_side_by_side_index_to_replace_index_with_errors()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("companies/1", null, new { Name = "HR" }, null);
                    Assert.Null(Indexes.WaitForIndexingErrors(store, new[] { IndexName }, errorsShouldExists: false));

                    store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition { Name = IndexName, Maps = { "from doc in docs let x = 0 select new { Total = 3/x };" } }}));
                    Indexes.WaitForIndexing(store);
                    Assert.Equal(1, Indexes.WaitForIndexingErrors(store, new[] { IndexName })[0].Errors.Length);

                    //store.DatabaseCommands.PutSideBySideIndexes(new[]
                    //{
                    //    new IndexToAdd
                    //    {
                    //        Name = IndexName,
                    //        Definition = new IndexDefinition {Maps = {"from doc in docs select new { Total = 3/1 };"}}
                    //    }
                    //});

                    SpinWait.SpinUntil(() => store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Length == 2);
                    Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));
                }
            }
        }

        [Fact(Skip = "RavenDB-5919")]
        public void can_create_side_by_side_index_with_errors_to_replace_index_with_errors()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("companies/1", null, new { Name = "HR" }, null);
                    Assert.Null(Indexes.WaitForIndexingErrors(store, new []{ IndexName }, errorsShouldExists: false));

                    store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition { Name = IndexName, Maps = { "from doc in docs let x = 0 select new { Total = 3/x };" } }}));
                    Indexes.WaitForIndexing(store);
                    Assert.Equal(1, Indexes.WaitForIndexingErrors(store, new[] { IndexName })[0].Errors.Length);

                    //store.DatabaseCommands.PutSideBySideIndexes(new[]
                    //{
                    //    new IndexToAdd
                    //    {
                    //        Name = IndexName,
                    //        Definition = new IndexDefinition {Maps = {"from doc in docs let x = 0 select new { Total = 3/x };"}}
                    //    }
                    //});

                    SpinWait.SpinUntil(() => store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Length == 2);
                    var errors = Indexes.WaitForIndexingErrors(store);
                    Assert.Equal(1, errors.Where(x => x.Name == IndexName).Sum(x => x.Errors.Length));
                    Assert.Equal(0, errors.Where(x => x.Name != IndexName).Sum(x => x.Errors.Length));
                }
            }
        }

        [Fact(Skip = "RavenDB-5919")]
        public void can_create_side_by_side_index_with_errors_to_replace_index()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("companies/1", null, new { Name = "HR" }, null);
                    Assert.Null(Indexes.WaitForIndexingErrors(store, new []{IndexName}, errorsShouldExists: false));
                    store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition { Name = IndexName,
                        Maps = { "from doc in docs select new { Total = 3/1 };" }} }));
                    Indexes.WaitForIndexing(store);
                    Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));

                    //store.DatabaseCommands.PutSideBySideIndexes(new[]
                    //{
                    //    new IndexToAdd
                    //    {
                    //        Name = IndexName,
                    //        Definition = new IndexDefinition { Maps = {  "from doc in docs let x = 0 select new { Total = 3/x };" } }
                    //    }
                    //});

                    SpinWait.SpinUntil(() => store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Length == 2);
                    var errors = Indexes.WaitForIndexingErrors(store);
                    Assert.Equal(1, errors.Where(x => x.Name == IndexName).Sum(x => x.Errors.Length));
                    Assert.Equal(0, errors.Where(x => x.Name != IndexName).Sum(x => x.Errors.Length));
                }
            }
        }
    }
}
