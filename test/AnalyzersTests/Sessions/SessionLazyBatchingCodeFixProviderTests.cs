using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Raven.Analyzers.CodeFixes.Sessions;
using Raven.Analyzers.Sessions;
using Xunit;

namespace AnalyzersTests.Sessions
{
    public class SessionLazyBatchingCodeFixProviderTests
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
        public async Task TwoLoads_Transforms_To_Lazy()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        var user = session.Load<User>(userId);
        var order = session.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // Should transform to lazy API
            Assert.Contains("session.Advanced.Lazily.Load<User>(userId)", fixed_code);
            Assert.Contains("session.Advanced.Lazily.Load<Order>(orderId)", fixed_code);
            // Reading the first .Value dispatches the whole batch, so no explicit Execute call.
            Assert.DoesNotContain("ExecuteAllPendingLazyOperations", fixed_code);
            Assert.Contains("var user = lazyUser.Value;", fixed_code);
            Assert.Contains("var order = lazyOrder.Value;", fixed_code);
        }

        [Fact]
        public async Task ExplicitlyTypedLoads_Keep_Lazy_As_Var_But_Restore_Declared_Type_On_Extraction()
        {
            // The rewritten lazy local holds a Lazy<T>, so the original explicit type ('User') must
            // be replaced with 'var' on the lazy declaration. The .Value extraction, however, restores
            // the original declared type: .Value is exactly the loaded type, so 'User user = ...' both
            // compiles and round-trips the user's explicit typing.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        User user = session.Load<User>(userId);
        Order order = session.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // Lazy declarations are emitted as 'var', not the original explicit type.
            Assert.Contains("var lazyUser = session.Advanced.Lazily.Load<User>(userId);", fixed_code);
            Assert.Contains("var lazyOrder = session.Advanced.Lazily.Load<Order>(orderId);", fixed_code);
            Assert.DoesNotContain("User lazyUser", fixed_code);
            Assert.DoesNotContain("Order lazyOrder", fixed_code);
            // Value extractions restore the original explicit declared type.
            Assert.Contains("User user = lazyUser.Value;", fixed_code);
            Assert.Contains("Order order = lazyOrder.Value;", fixed_code);
        }

        [Fact]
        public async Task QueryAndLoad_Transforms_To_Lazy()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string managerId)
    {
        var employees = session.Query<Employee>().Where(e => e.Active).ToList();
        var manager = session.Load<User>(managerId);
    }
}

class Employee { public string Id { get; set; } public bool Active { get; set; } }
class User { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // Query should use .Lazily()
            Assert.Contains(".Lazily()", fixed_code);
            // Load should use Lazily.Load
            Assert.Contains("session.Advanced.Lazily.Load<User>(managerId)", fixed_code);
            Assert.DoesNotContain("ExecuteAllPendingLazyOperations", fixed_code);
            // Value extractions
            Assert.Contains("var employees = lazyEmployees.Value.ToList();", fixed_code);
            Assert.Contains("var manager = lazyManager.Value;", fixed_code);
        }

        [Fact]
        public async Task NonConsecutive_Offers_No_Fix()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        var user = session.Load<User>(userId);
        var x = 42;  // Non-batchable statement between them
        var order = session.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            // Should throw because no fix is available (non-consecutive)
            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source));
        }

        [Fact]
        public async Task TwoAsyncLoads_Transforms_To_Lazy()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session, string userId, string orderId)
    {
        var user = await session.LoadAsync<User>(userId);
        var order = await session.LoadAsync<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // Async sessions expose IAsyncLazySessionOperations.LoadAsync, returning Lazy<Task<T>>.
            Assert.Contains("session.Advanced.Lazily.LoadAsync<User>(userId)", fixed_code);
            Assert.Contains("session.Advanced.Lazily.LoadAsync<Order>(orderId)", fixed_code);
            // Awaiting the first .Value dispatches the whole batch async, so no explicit Execute call.
            Assert.DoesNotContain("ExecuteAllPendingLazyOperations", fixed_code);
            // .Value is a Task<T> for async loads, so it must be awaited.
            Assert.Contains("var user = await lazyUser.Value;", fixed_code);
            Assert.Contains("var order = await lazyOrder.Value;", fixed_code);
        }

        [Fact]
        public async Task TwoLoads_OnDifferentSessions_Offers_No_Fix()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session1, IDocumentSession session2, string userId, string orderId)
    {
        var user = session1.Load<User>(userId);
        var order = session2.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            // Analyzer reports nothing for cross-session loads, so the harness throws before
            // a fix is even attempted. This documents that the fix never engages here.
            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source));
        }

        [Fact]
        public async Task AsyncQueryAndAsyncLoad_Transforms_To_Lazy()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session, string managerId)
    {
        var employees = await session.Query<Employee>().Where(e => e.Active).ToListAsync();
        var manager = await session.LoadAsync<User>(managerId);
    }
}

class Employee { public string Id { get; set; } public bool Active { get; set; } }
class User { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // Async query must register via LazilyAsync (returns Lazy<Task<IEnumerable<T>>>) so the
            // batch is dispatched asynchronously when the .Value is awaited.
            Assert.Contains(".LazilyAsync()", fixed_code);
            // Load on async session should use Lazily.LoadAsync (returning Lazy<Task<T>>)
            Assert.Contains("session.Advanced.Lazily.LoadAsync<User>(managerId)", fixed_code);
            // Awaiting the first .Value dispatches the whole batch async, so no explicit Execute call.
            Assert.DoesNotContain("ExecuteAllPendingLazyOperations", fixed_code);
            // Async query .Value is Task<IEnumerable<T>> — await then materialize: (await x.Value).ToList().
            Assert.Contains("var employees = (await lazyEmployees.Value).ToList();", fixed_code);
            // Async load .Value is Task<T> — must be awaited.
            Assert.Contains("var manager = await lazyManager.Value;", fixed_code);
        }

        [Fact]
        public async Task NameCollision_Generates_SuffixedName()
        {
            // lazyUser is already declared in the method; the fix must produce lazyUser2 to avoid collision
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        var lazyUser = 42;
        var user = session.Load<User>(userId);
        var order = session.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // lazyUser is taken → must use lazyUser2
            Assert.Contains("lazyUser2", fixed_code);
            Assert.DoesNotContain("var lazyUser =", fixed_code.Replace("var lazyUser = 42;", ""));
            Assert.DoesNotContain("ExecuteAllPendingLazyOperations", fixed_code);
            Assert.Contains("var user = lazyUser2.Value;", fixed_code);
        }

        [Fact]
        public async Task Comment_On_Batchable_Statement_Is_Preserved()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        // load the user
        var user = session.Load<User>(userId);
        var order = session.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // Comment must appear exactly once — on the renamed lazy declaration, not copied
            // onto a .Value extraction statement.
            int commentCount = 0;
            int searchFrom = 0;
            while (true)
            {
                int idx = fixed_code.IndexOf("// load the user", searchFrom, System.StringComparison.Ordinal);
                if (idx < 0)
                    break;
                commentCount++;
                searchFrom = idx + 1;
            }
            Assert.Equal(1, commentCount);

            // Comment must sit before the Lazily.Load call, which in turn precedes the .Value
            // extraction section (registrations never contain ".Value").
            int commentPos = fixed_code.IndexOf("// load the user", System.StringComparison.Ordinal);
            int lazyLoadPos = fixed_code.IndexOf("Lazily.Load<User>", System.StringComparison.Ordinal);
            int firstValuePos = fixed_code.IndexOf(".Value", System.StringComparison.Ordinal);
            Assert.True(commentPos < lazyLoadPos, "Comment should precede the Lazily.Load line");
            Assert.True(lazyLoadPos < firstValuePos, "Lazily.Load should precede the .Value extraction");

            Assert.DoesNotContain("ExecuteAllPendingLazyOperations", fixed_code);
        }

        [Fact]
        public async Task Comment_On_Second_Batchable_Statement_Is_Not_Duplicated()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        var user = session.Load<User>(userId);
        // load the order
        var order = session.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // Comment must appear exactly once — on the renamed lazy declaration for the second Load.
            int commentCount = 0;
            int searchFrom = 0;
            while (true)
            {
                int idx = fixed_code.IndexOf("// load the order", searchFrom, System.StringComparison.Ordinal);
                if (idx < 0)
                    break;
                commentCount++;
                searchFrom = idx + 1;
            }
            Assert.Equal(1, commentCount);

            Assert.DoesNotContain("ExecuteAllPendingLazyOperations", fixed_code);
        }

        [Fact]
        public async Task Playground_Shape_Multi_Comment_Block_And_Trailing_Comments_Not_Duplicated()
        {
            // Mirrors the exact shape of RVN012_SessionLazyBatching.BadExample in the playground:
            // three stacked single-line comments before stmt1, trailing same-line comments on each Load.
            // The trailing comments consume the EOL into `;`'s trailing trivia, leaving stmt2's
            // leading trivia as pure whitespace (no EOL), which previously caused a second formatting
            // bug: the second extraction was jammed onto the same line as the first.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        // warning RVN012: 'Load' is an eager session operation. This method contains
        //   multiple independent session operations; use session.Advanced.Lazily or
        //   query.Lazily() to batch them into a single server round-trip.
        var user  = session.Load<User>(userId);   // round-trip 1
        var order = session.Load<Order>(orderId); // round-trip 2
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // Each comment line must appear exactly once in the output.
            string[] comments =
            [
                "// warning RVN012: 'Load' is an eager session operation. This method contains",
                "//   multiple independent session operations; use session.Advanced.Lazily or",
                "//   query.Lazily() to batch them into a single server round-trip.",
                "// round-trip 1",
                "// round-trip 2",
            ];
            foreach (string comment in comments)
            {
                int count = 0;
                int pos = 0;
                while (true)
                {
                    int idx = fixed_code.IndexOf(comment, pos, System.StringComparison.Ordinal);
                    if (idx < 0) break;
                    count++;
                    pos = idx + 1;
                }
                Assert.True(count == 1, $"Expected comment to appear exactly once but found {count}: {comment}");
            }

            // No comment must appear in the synthesised .Value extraction section. Registrations use
            // ".Lazily"/".Lazily.Load" and never ".Value", so the first ".Value" marks where the
            // comment-free extraction section starts.
            int firstValuePos = fixed_code.IndexOf(".Value", System.StringComparison.Ordinal);
            Assert.True(firstValuePos >= 0, ".Value extraction not found in fixed output");
            string afterFirstValue = fixed_code.Substring(firstValuePos);
            Assert.DoesNotContain("//", afterFirstValue);

            // Extractions must be on separate lines — not jammed onto one line.
            Assert.DoesNotContain("Value;        var", fixed_code);
            Assert.DoesNotContain("Value;    var", fixed_code);

            // The simplified fix relies on .Value to dispatch the batch — no explicit Execute call.
            Assert.DoesNotContain("ExecuteAllPendingLazyOperations", fixed_code);
        }

        [Fact]
        public async Task DependentLoad_Within_Batch_Offers_No_Fix()
        {
            // The middle Load depends on the first Load's result (user.ManagerId). Batching would
            // rewrite all three and materialize 'user' only after the batch, so 'user.ManagerId'
            // would reference 'user' before it is declared (CS0841). The fix must bail.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId)
    {
        var user = session.Load<User>(userId);
        var manager = session.Load<User>(user.ManagerId);
        var other = session.Load<User>(userId);
    }
}

class User { public string Id { get; set; } public string ManagerId { get; set; } }
";

            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source));
        }

        [Fact]
        public async Task DependentQuery_On_Prior_Load_Offers_No_Fix()
        {
            // The query depends on the prior Load's result (user.Id). Batching would materialize
            // 'user' only after the batch, so the query's lazy initializer would reference 'user'
            // before it is declared. The fix must bail rather than emit uncompilable code.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId)
    {
        var user = session.Load<User>(userId);
        var orders = session.Query<Order>().Where(o => o.OwnerId == user.Id).ToList();
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } public string OwnerId { get; set; } }
";

            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source));
        }

        [Fact]
        public async Task NameCollision_With_Method_Parameter_Generates_SuffixedName()
        {
            // 'lazyUser' is a method parameter (not a local), so it is not a descendant of the body
            // block. The generated name must still avoid it (it would otherwise be CS0136), which
            // requires consulting the semantic model for in-scope symbols.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId, User lazyUser)
    {
        var user = session.Load<User>(userId);
        var order = session.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            // 'lazyUser' is taken by the parameter → must use 'lazyUser2'.
            Assert.Contains("lazyUser2", fixed_code);
            Assert.DoesNotContain("var lazyUser =", fixed_code);
            Assert.Contains("var user = lazyUser2.Value;", fixed_code);
        }

        [Fact]
        public async Task TypeArgument_Sharing_BatchedVariable_Name_Still_Offers_Fix()
        {
            // The second Load's type argument 'User' is an identifier that matches the local named
            // 'User', but it is a TYPE, not the batched local — there is no real dependency. The
            // dependency check must compare symbols (not names) so it does not falsely bail here.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id1, string id2)
    {
        var User = session.Load<User>(id1);
        var other = session.Load<User>(id2);
    }
}

class User { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            Assert.Contains("var lazyUser = session.Advanced.Lazily.Load<User>(id1);", fixed_code);
            Assert.Contains("var lazyOther = session.Advanced.Lazily.Load<User>(id2);", fixed_code);
            Assert.Contains("var User = lazyUser.Value;", fixed_code);
        }

        [Fact]
        public async Task NameCollision_With_Catch_Variable_Generates_SuffixedName()
        {
            // 'lazyUser' is a catch-clause variable in a nested scope; LookupSymbols at the block
            // start cannot see it, so the syntactic walk must reserve it. Otherwise the generated
            // 'lazyUser' local collides with the catch variable (CS0136).
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        var user = session.Load<User>(userId);
        var order = session.Load<Order>(orderId);
        try { } catch (Exception lazyUser) { Console.WriteLine(lazyUser); }
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            string fixed_code = await RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source);

            Assert.Contains("lazyUser2", fixed_code);
            Assert.DoesNotContain("var lazyUser =", fixed_code);
            Assert.Contains("var user = lazyUser2.Value;", fixed_code);
        }

        [Fact]
        public async Task IncludeStyleLoads_Offer_No_Fix()
        {
            // Regression: the include-builder Load overload (Load<T>(id, Action<IIncludeBuilder<T>>))
            // has no lazy counterpart. Previously the analyzer flagged it and the fix produced
            // session.Advanced.Lazily.Load<T>(id, i => i.IncludeDocuments(...)) — which does not
            // compile (Lazily.Load takes an Action<T> onEval, not an include builder). The analyzer no
            // longer flags it, so the fix never engages and the harness throws (no diagnostic).
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string managerId)
    {
        var user = session.Load<User>(userId, i => i.IncludeDocuments(x => x.ManagerId));
        var manager = session.Load<User>(managerId, i => i.IncludeDocuments(x => x.ManagerId));
    }
}

class User { public string Id { get; set; } public string ManagerId { get; set; } }
";

            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source));
        }

        [Fact]
        public async Task AsyncQuery_With_CancellationToken_Argument_Offers_No_Fix()
        {
            // The lazy rewrite cannot carry the ToListAsync(token) argument without silently dropping
            // the CancellationToken, so both the analyzer and the code fix exclude an arg-bearing
            // materializer. With only the Load left, there are fewer than two batchable operations,
            // so the analyzer reports nothing and no fix is offered (the harness throws).
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session, string managerId, System.Threading.CancellationToken token)
    {
        var employees = await session.Query<Employee>().Where(e => e.Active).ToListAsync(token);
        var manager = await session.LoadAsync<User>(managerId);
    }
}

class Employee { public string Id { get; set; } public bool Active { get; set; } }
class User { public string Id { get; set; } }
";

            await Assert.ThrowsAsync<System.InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SessionLazyBatchingAnalyzer, SessionLazyBatchingCodeFixProvider>(source));
        }
    }
}
