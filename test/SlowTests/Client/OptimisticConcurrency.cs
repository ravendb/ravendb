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
                    var e = Assert.Throws<ConcurrencyException>(() =>
                    {
                        newSession.SaveChanges();
                    });
                    Assert.StartsWith("Document Foos/1 has change vector A:1-", e.Message);
                }
            }
        }

        [Theory]
        [InlineData("")]
        [InlineData("A:1-dummyDbId")]
        public void store_should_throw_exception_if_doc_exists_and_optimistic_concurrency_is_enabled_with_invalid_change_vector(string changeVector)
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
                    newSession.Store(foo, changeVector: changeVector, fooId);
                    var e = Assert.Throws<ConcurrencyException>(() =>
                    {
                        newSession.SaveChanges();
                    });
                    Assert.StartsWith("Document Foos/1 has change vector A:1-", e.Message);
                }
            }
        }

        [Fact]
        public void delete_should_throw_exception_if_doc_exists_and_optimistic_concurrency_is_enabled()
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
                    newSession.Delete(fooId, "A:1-dummy");
                    var e = Assert.Throws<ConcurrencyException>(() =>
                    {
                        newSession.SaveChanges();
                    });
                    Assert.StartsWith("Document Foos/1 has change vector A:1-", e.Message);
                }
            }
        }
    }
}
