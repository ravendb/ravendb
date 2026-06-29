using System.Threading.Tasks;
using AnalyzersTests.Framework;
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
        public async Task OpenSession_Transforms_Using_Actual_Run_Parameter_Name()
        {
            // The Run lambda parameter is not named "batch"; the rewrite must use the actual name.
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(x =>
        {
            var session = store.OpenSession();
            var doc = session.Load<Document>(""id"");
        });
    }
}

class Document { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source);

            Assert.Contains("var session = x.OpenSession()", fixed_code);
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
        public async Task OpenSessionWithDatabaseName_FixBails()
        {
            // store.OpenSession(database) is flagged by RVN011, but SubscriptionBatch exposes only
            // OpenSession() / OpenSession(SessionOptions) — there is no OpenSession(string) overload.
            // Rewriting to batch.OpenSession(""MyDatabase"") would not compile, so the fix must bail
            // while the diagnostic still stands (the misuse must be restructured by hand).
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            var session = store.OpenSession(""MyDatabase"");
            var doc = session.Load<Document>(""id"");
        });
    }
}

class Document { public string Id { get; set; } }
";

            System.InvalidOperationException ex = await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source));

            // The diagnostic fired (the harness got past the "no diagnostic" guard); the fix declined.
            Assert.Contains("No code fixes were registered", ex.Message);
        }

        [Fact]
        public async Task OpenAsyncSessionWithDatabaseName_FixBails()
        {
            // Same as the sync case for the async overload: store.OpenAsyncSession(string database)
            // has no batch counterpart, so the fix bails while the diagnostic stands.
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            var session = store.OpenAsyncSession(""MyDatabase"");
        });
    }
}

class Document { public string Id { get; set; } }
";

            System.InvalidOperationException ex = await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source));

            Assert.Contains("No code fixes were registered", ex.Message);
        }

        [Fact]
        public async Task OpenSessionWithNamedSessionOptionsArgument_FixBails()
        {
            // store.OpenSession(sessionOptions: opts) compiles against IDocumentStore (its parameter is
            // named 'sessionOptions'), but SubscriptionBatch.OpenSession's parameter is named 'options'.
            // Swapping only the receiver would yield batch.OpenSession(sessionOptions: opts) — CS1739.
            // The fix must bail on a named argument while the diagnostic still stands.
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            var session = store.OpenSession(sessionOptions: new SessionOptions { NoCaching = true });
            var doc = session.Load<Document>(""id"");
        });
    }
}

class Document { public string Id { get; set; } }
";

            System.InvalidOperationException ex = await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source));

            Assert.Contains("No code fixes were registered", ex.Message);
        }

        [Fact]
        public async Task OpenSessionWithPositionalSessionOptions_Transforms_To_Batch()
        {
            // A positional SessionOptions argument is batch-compatible (SubscriptionBatch.OpenSession
            // accepts SessionOptions), so the fix still applies — guards against the named-argument
            // rejection being too broad.
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Document> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            var session = store.OpenSession(new SessionOptions { NoCaching = true });
            var doc = session.Load<Document>(""id"");
        });
    }
}

class Document { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source);

            Assert.Contains("var session = batch.OpenSession(new SessionOptions", fixed_code);
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
