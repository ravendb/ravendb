// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases;
using Raven.Client.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4904 : RavenNewTestBase
    {
        private const string IndexName = "testIndex";

        [Fact(Skip = "RavenDB-5919")]
        public void can_create_side_by_side_index_to_replace_index_with_errors()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("companies/1", null, new { Name = "HR" }, null);
                    Assert.Equal(0, store.Admin.Send(new GetIndexErrorsOperation(new[] { IndexName }))[0].Errors.Length);

                    store.Admin.Send(new PutIndexOperation(IndexName, new IndexDefinition { Maps = { "from doc in docs let x = 0 select new { Total = 3/x };" } }));
                    WaitForIndexing(store);
                    Assert.Equal(1, store.Admin.Send(new GetIndexErrorsOperation(new[] { IndexName }))[0].Errors.Length);

                    //store.DatabaseCommands.PutSideBySideIndexes(new[]
                    //{
                    //    new IndexToAdd
                    //    {
                    //        Name = IndexName,
                    //        Definition = new IndexDefinition {Maps = {"from doc in docs select new { Total = 3/1 };"}}
                    //    }
                    //});

                    SpinWait.SpinUntil(() => store.Admin.Send(new GetStatisticsOperation()).Indexes.Length == 2);
                    Assert.Equal(0, store.Admin.Send(new GetIndexErrorsOperation()).Where(x => x.Name == IndexName).Sum(x => x.Errors.Length));
                    Assert.Equal(0, store.Admin.Send(new GetIndexErrorsOperation()).Where(x => x.Name != IndexName).Sum(x => x.Errors.Length));
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
                    Assert.Equal(0, store.Admin.Send(new GetIndexErrorsOperation(new[] { IndexName }))[0].Errors.Length);

                    store.Admin.Send(new PutIndexOperation(IndexName, new IndexDefinition { Maps = { "from doc in docs let x = 0 select new { Total = 3/x };" } }));
                    WaitForIndexing(store);
                    Assert.Equal(1, store.Admin.Send(new GetIndexErrorsOperation(new[] { IndexName }))[0].Errors.Length);

                    //store.DatabaseCommands.PutSideBySideIndexes(new[]
                    //{
                    //    new IndexToAdd
                    //    {
                    //        Name = IndexName,
                    //        Definition = new IndexDefinition {Maps = {"from doc in docs let x = 0 select new { Total = 3/x };"}}
                    //    }
                    //});

                    SpinWait.SpinUntil(() => store.Admin.Send(new GetStatisticsOperation()).Indexes.Length == 2);
                    Assert.Equal(1, store.Admin.Send(new GetIndexErrorsOperation()).Where(x => x.Name == IndexName).Sum(x => x.Errors.Length));
                    Assert.Equal(0, store.Admin.Send(new GetIndexErrorsOperation()).Where(x => x.Name != IndexName).Sum(x => x.Errors.Length));
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
                    Assert.Equal(0, store.Admin.Send(new GetIndexErrorsOperation(new[] { IndexName }))[0].Errors.Length);

                    store.Admin.Send(new PutIndexOperation(IndexName, new IndexDefinition { Maps = { "from doc in docs select new { Total = 3/1 };" } }));
                    WaitForIndexing(store);
                    Assert.Equal(0, store.Admin.Send(new GetIndexErrorsOperation(new[] { IndexName }))[0].Errors.Length);

                    //store.DatabaseCommands.PutSideBySideIndexes(new[]
                    //{
                    //    new IndexToAdd
                    //    {
                    //        Name = IndexName,
                    //        Definition = new IndexDefinition { Maps = {  "from doc in docs let x = 0 select new { Total = 3/x };" } }
                    //    }
                    //});

                    SpinWait.SpinUntil(() => store.Admin.Send(new GetStatisticsOperation()).Indexes.Length == 2);
                    Assert.Equal(1, store.Admin.Send(new GetIndexErrorsOperation()).Where(x => x.Name == IndexName).Sum(x => x.Errors.Length));
                    Assert.Equal(0, store.Admin.Send(new GetIndexErrorsOperation()).Where(x => x.Name != IndexName).Sum(x => x.Errors.Length));
                }
            }
        }
    }
}
