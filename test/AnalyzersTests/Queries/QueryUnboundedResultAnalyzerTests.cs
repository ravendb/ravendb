using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Queries;
using Xunit;

namespace AnalyzersTests.Queries
{
    public class QueryUnboundedResultAnalyzerTests
    {
        private const string CommonUsings = @"
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
";

        [Fact]
        public async Task QueryToList_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().ToList();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
            Assert.Equal(DiagnosticSeverity.Info, diagnostics[0].Severity);
        }

        [Fact]
        public async Task QueryToArray_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().ToArray();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [Fact]
        public async Task QueryToListAsync_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        var users = await session.Query<User>().ToListAsync();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [Fact]
        public async Task QueryToArrayAsync_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        var users = await session.Query<User>().ToArrayAsync();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [Fact]
        public async Task QueryWithWhereAndNoTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Where(u => u.Active).ToList();
    }
}

class User { public string Id { get; set; } public bool Active { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [Fact]
        public async Task QueryWithTake_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Take(10).ToList();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task QueryWithWhereThenTake_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Where(u => u.Active).Take(10).ToList();
    }
}

class User { public string Id { get; set; } public bool Active { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task QueryWithSkipButNoTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Skip(5).ToList();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [Fact]
        public async Task QueryFirst_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var user = session.Query<User>().First();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task QueryCount_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var count = session.Query<User>().Count();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task LocalListToList_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run()
    {
        var list = new List<User>();
        var users = list.Where(x => x.Active).ToList();
    }
}

class User { public string Id { get; set; } public bool Active { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task MultipleUnboundedQueries_Reports_Multiple_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().ToList();
        var orders = session.Query<Order>().ToArray();
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d => Assert.Equal(DiagnosticIds.QueryUnboundedResult, d.Id));
        }
    }
}
