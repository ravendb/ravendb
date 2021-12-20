using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDb17419 : RavenTestBase
    {
        public RavenDb17419(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ThrowsWhenComparingWholeDocument()
        {
            using var store = GetDocumentStore();
            using(var session = store.OpenSession())
            {
                session.Store(new Employee(){ Name = "123" });
                session.SaveChanges();
            }

            using(var session = store.OpenSession())
            {
                var exception = Assert.Throws<InvalidQueryException>(() => session.Query<Employee>().Where(e => e != null).ToList());
                Assert.Equal(RavenQueryProviderProcessor<object>.WholeDocumentComparisonExceptionMessage, exception.Message);
                
                exception = Assert.Throws<InvalidQueryException>(() => session.Query<Employee>().Where(e => e == null).ToList());
                Assert.Equal(RavenQueryProviderProcessor<object>.WholeDocumentComparisonExceptionMessage, exception.Message);
            }
        }
        
        private class Employee
        {
            public string Name { get; set; }
        }
    }
}
