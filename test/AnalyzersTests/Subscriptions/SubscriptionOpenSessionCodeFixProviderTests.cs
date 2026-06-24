using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Raven.Analyzers;
using Raven.Analyzers.CodeFixes.Subscriptions;
using Raven.Analyzers.Subscriptions;
using Xunit;

namespace AnalyzersTests.Subscriptions
{
    public class SubscriptionOpenSessionCodeFixProviderTests
    {
        private const string CommonUsings = @"
using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
";

        [Fact]
        public async Task OpenSession_Transforms_To_Batch()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            var session = store.OpenSession();
            var doc = session.Load<Document>(""id"");
        });
    }
}

class Document { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source);

            Assert.Contains("var session = batch.OpenSession()", fixed_code);
        }

        [Fact]
        public async Task OpenSessionWithOptions_Transforms_To_Batch()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            var options = new SessionOptions { NoCaching = true };
            var session = store.OpenSession(options);
            var doc = session.Load<Document>(""id"");
        });
    }
}

class Document { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source);

            Assert.Contains("var session = batch.OpenSession(options)", fixed_code);
        }

        [Fact]
        public async Task OpenAsyncSession_Transforms_To_Batch()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            var session = store.OpenAsyncSession();
            var doc = session.LoadAsync<Document>(""id"");
        });
    }
}

class Document { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source);

            Assert.Contains("var session = batch.OpenAsyncSession()", fixed_code);
        }

        [Fact]
        public async Task OpenAsyncSessionWithOptions_Transforms_To_Batch()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            var options = new SessionOptions { NoCaching = true };
            var session = store.OpenAsyncSession(options);
            var doc = session.LoadAsync<Document>(""id"");
        });
    }
}

class Document { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source);

            Assert.Contains("var session = batch.OpenAsyncSession(options)", fixed_code);
        }

        [Fact]
        public async Task OpenSession_InDeferredAction_FixBails()
        {
            // Diagnostic fires (store.OpenSession() is inside the Run lambda) but the fix refuses to
            // rewrite it: the call is inside a deferred Action nested within the batch lambda, so the
            // session may outlive the batch. No code action is registered.
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            Action deferred = () => store.OpenSession();
        });
    }
}

class Document { public string Id { get; set; } }
";

            // ApplyFixAsync throws when no code action is registered (the fix bails on a nested lambda)
            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source));
        }

        [Fact]
        public async Task OpenSession_InNonStaticLocalFunction_Transforms_To_Batch()
        {
            // A non-static local function captures the Run lambda's batch parameter, so the
            // rewrite to batch.OpenSession() is valid and the fix applies.
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            void Process()
            {
                var session = store.OpenSession();
                var doc = session.Load<Document>(""id"");
            }
            Process();
        });
    }
}

class Document { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source);

            Assert.Contains("var session = batch.OpenSession()", fixed_code);
        }

        [Fact]
        public async Task OpenSession_InStaticLocalFunction_FixBails()
        {
            // A static local function cannot capture batch, so the analyzer does not flag it and
            // no code action is registered. ApplyFixAsync throws when there is nothing to apply.
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            static void Process(IDocumentStore s)
            {
                var session = s.OpenSession();
            }
            Process(store);
        });
    }
}

class Document { public string Id { get; set; } }
";

            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source));
        }

        [Fact]
        public async Task OpenSession_InNestedLambda_FixBails()
        {
            // store.OpenSession() inside a nested lambda may outlive the batch; the fix refuses to
            // rewrite it so the user can decide whether batch.OpenSession() is appropriate.
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            System.Threading.Tasks.Task.Run(() =>
            {
                var session = store.OpenSession();
            });
        });
    }
}

class Document { public string Id { get; set; } }
";

            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source));
        }

        [Fact]
        public async Task OpenSession_InNestedAnonymousMethod_FixBails()
        {
            // store.OpenSession() inside a nested anonymous method (delegate { ... }) may outlive
            // the batch, just like a nested lambda; the fix refuses to rewrite it.
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            System.Action action = delegate
            {
                var session = store.OpenSession();
            };
            action();
        });
    }
}

class Document { public string Id { get; set; } }
";

            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source));
        }
    }
}
