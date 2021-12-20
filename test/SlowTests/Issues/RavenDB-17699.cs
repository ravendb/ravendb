using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17699 : RavenTestBase
    {
        public RavenDB_17699(ITestOutputHelper output) : base(output)
        {
        }

        private record Item(string name);

        [Fact]
        public void MultipleConditionalGetQueries()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                s.Store(new Item("Book"), "items/book");
                s.SaveChanges();
            }
         

            using (var s = store.OpenSession())
            {
                var bookLazy = s.Advanced.Lazily. ConditionalLoad<Item>("items/book", "bad-value");
                s.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                Assert.NotNull(bookLazy.Value.Entity);
            }
            
            using (var s = store.OpenSession())
            {
                var bookLazy = s.Advanced.Lazily. ConditionalLoad<Item>("items/book", "bad-value");
                s.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                Assert.NotNull(bookLazy.Value.Entity);
            }
        }
    }
}
