using FastTests;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Client
{
    public class OptimisticConcurrency : RavenTestBase
    {
        public class Foo
        {
            public string Name; 
        }
        
        [Fact]
        public void store_should_throw_exception_if_doc_exists_and_optimistic_concurrency_is_enabled()
        {
            using (var store = GetDocumentStore())
            {
                string fooId = "Foos/1";
                
                using (var session = store.OpenSession())
                {
                    var foo = new Foo { Name = "One" };
                    session.Store(foo, fooId);
                    session.SaveChanges();
                }
                using (var newSession = store.OpenSession())
                {
                    newSession.Advanced.UseOptimisticConcurrency = true;
                    var foo = new Foo { Name = "Two" };
                    newSession.Store(foo, fooId);
                    Assert.Throws<ConcurrencyException>(() =>
                    {
                        newSession.SaveChanges();
                    });
                }
            }
        }
    }
}
