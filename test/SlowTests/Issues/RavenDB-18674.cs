using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Queries.Timings;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Raven.Server.Config;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18674 : RavenTestBase
    {
        public RavenDB_18674(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WrongQueryExpressionWhenDeclaringFunction()
        {
            var parser = new QueryParser();
            parser.Init("DECLARE function output(o) { return { Total: o.Total }; }"+
                            "from index 'Orders/ByCompany' as o " +
                            "where Total > 1000 select output(o)");
            var query = parser.Parse(QueryType.Select);
            var s = query.ToString();
            Assert.False(s.Contains("function output function output(o)"), "duplicate of \'function output\' in query.ToString()");

            using var store = GetDocumentStore();
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    session.Store(new Item
                    {
                        Content = $"Item{i}"
                    });
                    session.SaveChanges();
                }
            }

            WaitForUserToContinueTheTest(store);
        }

        private class Item
        {
            public string Id { get; set; }
            public string Content { get; set; }
        }

    }

}
