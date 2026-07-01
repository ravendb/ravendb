using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Raven.Analyzers.CodeFixes.Sessions;
using Raven.Analyzers.Sessions;
using Xunit;

namespace AnalyzersTests.Sessions
{
    // Regression test: the lazy-batching code fix derives the generated lazy name from the identifier's
    // ValueText, so a verbatim identifier ('@int') yields the valid 'lazyInt' rather than the
    // uncompilable 'lazy@int', while the extraction re-declares the original verbatim identifier.
    public class SessionLazyBatchingVerbatimIdentifierRegressionTests
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
        public async Task Verbatim_Identifier_Load_Produces_Valid_Lazy_Name()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        var @int = session.Load<User>(userId);
        var order = session.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";
            string fixedCode = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            Assert.DoesNotContain("lazy@int", fixedCode);
            Assert.Contains("var lazyInt = session.Advanced.Lazily.Load<User>(userId);", fixedCode);
            Assert.Contains("var @int = lazyInt.Value;", fixedCode);
        }
    }
}
