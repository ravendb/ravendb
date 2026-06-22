using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Raven.Analyzers;
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
            Assert.Contains("session.Advanced.Eagerly.ExecuteAllPendingLazyOperations()", fixed_code);
            Assert.Contains("var user = lazyUser.Value;", fixed_code);
            Assert.Contains("var order = lazyOrder.Value;", fixed_code);
        }

        [Fact]
        public async Task ExplicitlyTypedLoads_Transform_To_Var_Lazy()
        {
            // The rewritten lazy local holds a Lazy<T>, so the original explicit type ('User')
            // must be replaced with 'var' or the generated code would not compile.
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
            // Value extractions restore the original variable names via var.
            Assert.Contains("var user = lazyUser.Value;", fixed_code);
            Assert.Contains("var order = lazyOrder.Value;", fixed_code);
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
            Assert.Contains("session.Advanced.Eagerly.ExecuteAllPendingLazyOperations()", fixed_code);
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
            // Execute should be async
            Assert.Contains("await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync()", fixed_code);
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

            // Query should use .Lazily() (the async ToListAsync becomes the sync Lazily registration)
            Assert.Contains(".Lazily()", fixed_code);
            // Load on async session should use Lazily.LoadAsync (returning Lazy<Task<T>>)
            Assert.Contains("session.Advanced.Lazily.LoadAsync<User>(managerId)", fixed_code);
            // Execute should be async
            Assert.Contains("await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync()", fixed_code);
            // Query .Value is IEnumerable<T> — call ToList() as before.
            Assert.Contains("var employees = lazyEmployees.Value.ToList();", fixed_code);
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
            Assert.Contains("session.Advanced.Eagerly.ExecuteAllPendingLazyOperations()", fixed_code);
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
            // onto the synthesised Execute call or the .Value extraction statement.
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

            // Comment must sit before the Lazily.Load call, not before the Execute or Value lines.
            int commentPos = fixed_code.IndexOf("// load the user", System.StringComparison.Ordinal);
            int lazyLoadPos = fixed_code.IndexOf("Lazily.Load<User>", System.StringComparison.Ordinal);
            int executePos  = fixed_code.IndexOf("ExecuteAllPendingLazyOperations", System.StringComparison.Ordinal);
            Assert.True(commentPos < lazyLoadPos, "Comment should precede the Lazily.Load line");
            Assert.True(lazyLoadPos < executePos,  "Lazily.Load should precede the Execute call");

            Assert.Contains("session.Advanced.Eagerly.ExecuteAllPendingLazyOperations()", fixed_code);
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

            Assert.Contains("session.Advanced.Eagerly.ExecuteAllPendingLazyOperations()", fixed_code);
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

            // No comment must appear after the execute statement (the synthesised section is comment-free).
            int executePos = fixed_code.IndexOf("ExecuteAllPendingLazyOperations", System.StringComparison.Ordinal);
            Assert.True(executePos >= 0, "ExecuteAllPendingLazyOperations not found in fixed output");
            string afterExecute = fixed_code.Substring(executePos);
            Assert.DoesNotContain("//", afterExecute);

            // Extractions must be on separate lines — not jammed onto one line.
            Assert.DoesNotContain("Value;        var", fixed_code);
            Assert.DoesNotContain("Value;    var", fixed_code);

            Assert.Contains("session.Advanced.Eagerly.ExecuteAllPendingLazyOperations()", fixed_code);
        }
    }
}
