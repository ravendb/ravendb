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

            // Async loads should become sync lazy registrations (lazy API is always sync)
            Assert.Contains("session.Advanced.Lazily.Load<User>(userId)", fixed_code);
            Assert.Contains("session.Advanced.Lazily.Load<Order>(orderId)", fixed_code);
            // Execute should be async
            Assert.Contains("await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync()", fixed_code);
            // Value extractions remain sync
            Assert.Contains("var user = lazyUser.Value;", fixed_code);
            Assert.Contains("var order = lazyOrder.Value;", fixed_code);
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

            // Query should use .Lazily() (removing the async variant)
            Assert.Contains(".Lazily()", fixed_code);
            // Load should use Lazily.Load
            Assert.Contains("session.Advanced.Lazily.Load<User>(managerId)", fixed_code);
            // Execute should be async
            Assert.Contains("await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync()", fixed_code);
            // Value extractions
            Assert.Contains("var employees = lazyEmployees.Value.ToList();", fixed_code);
            Assert.Contains("var manager = lazyManager.Value;", fixed_code);
        }
    }
}
