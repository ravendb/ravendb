using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers.CodeFixes.Sessions;
using Raven.Analyzers.Sessions;
using Xunit;

namespace AnalyzersTests.Sessions
{
    // Regression tests for the lazy-batching analyzer/code-fix correctness fixes:
    // only stable session instances are grouped, the declared type is preserved on the extraction,
    // and a mixed sync/async batch is never rewritten.
    public class SessionLazyBatchingRegressionTests
    {
        private const string CommonUsings = @"
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;
";

        [Fact]
        public async Task Loads_Through_A_Factory_Method_Are_Not_Grouped()
        {
            // GetSession() may return a different instance each call, so the two loads are not
            // provably on the same session and must not be flagged as batchable.
            const string source = CommonUsings + @"
class Test
{
    IDocumentSession GetSession() => null;
    void Run(string a, string b)
    {
        var x = GetSession().Load<Doc>(a);
        var y = GetSession().Load<Doc>(b);
    }
}
class Doc { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Loads_Through_A_Property_Are_Not_Grouped()
        {
            // A property getter may return a fresh session per access, so two loads through it are
            // not provably on the same instance.
            const string source = CommonUsings + @"
class Test
{
    IDocumentSession Session { get; set; }
    void Run(string a, string b)
    {
        var x = Session.Load<Doc>(a);
        var y = Session.Load<Doc>(b);
    }
}
class Doc { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Loads_Through_A_Field_Are_Still_Flagged()
        {
            // A field is a stable instance, so two loads through it are genuinely batchable.
            const string source = CommonUsings + @"
class Test
{
    IDocumentSession _session;
    void Run(string a, string b)
    {
        var x = _session.Load<Doc>(a);
        var y = _session.Load<Doc>(b);
    }
}
class Doc { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.NotEmpty(diagnostics);
        }

        [Fact]
        public async Task Explicit_Enumerable_Query_Type_Is_Preserved_On_Extraction()
        {
            // The .Value extraction must restore the original IEnumerable<T> declaration, not infer
            // List<T> via 'var', or a later reassignment of the variable would fail to compile.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        IEnumerable<Doc> docs = session.Query<Doc>().Where(d => d.Active).ToList();
        var single = session.Load<Doc>(id);
    }
}
class Doc { public string Id { get; set; } public bool Active { get; set; } }
";
            string fixedCode = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            Assert.Contains("IEnumerable<Doc> docs = lazyDocs.Value.ToList();", fixedCode);
        }

        [Fact]
        public async Task Mixed_Sync_And_Async_Batch_Offers_No_Fix()
        {
            // Reading a sync .Value first would force the async-registered op to dispatch through the
            // blocking path (sync-over-async). The fix must bail rather than rewrite a mixed batch.
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session, string id)
    {
        var docs = session.Query<Doc>().ToList();
        var single = await session.LoadAsync<Doc>(id);
    }
}
class Doc { public string Id { get; set; } }
";
            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source));

            Assert.Contains("No code fixes", ex.Message);
        }
    }
}
