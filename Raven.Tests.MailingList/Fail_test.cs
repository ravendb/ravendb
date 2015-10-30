using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class when_index_failing : RavenTestBase
    {
        public class Foo
        {
            public decimal Bar { get; set; }
        }
    
        [Fact]
        public void Document_with_big_decimal_saving_test()
        {
            var documentStore = this.NewDocumentStore(true);
            string id = "ID";
            var foo = new Foo
            {
                Bar = 9999999999999999999999999999M
            };
            
            using (var session = documentStore.OpenSession())
            {
                session.Store(foo, id);
                session.SaveChanges();
            }
            
            Foo resultFoo = null;
            
            using (var session = documentStore.OpenSession())
            {
                resultFoo = session.Load<Foo>(id);
            }
            
            Assert.Equal(resultFoo.Bar, foo.Bar);
        }
        
    }
}
