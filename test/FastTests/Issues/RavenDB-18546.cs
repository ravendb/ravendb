using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_18546 : RavenTestBase
{
    public RavenDB_18546(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void Should_Resolve_Class_Type()
    {
        const string id1 = "products/1";
        const string id2 = "products/2";
        using (var store = GetDocumentStore(new Options
               {
                   ModifyDocumentStore = s => s.Conventions.ResolveTypeFromClrTypeName = _ => typeof(MyClass)
               }))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new CustomProduct
                {
                    Name = "Arava"
                }, id1);
                
                session.Store(new CustomProduct
                {
                    Name = "Arava"
                }, id2);
                session.SaveChanges();
            }
            using (var session = store.OpenSession())
            {
                var prod1 = session.Load<Product>(id1);
                Assert.IsType<MyClass>(prod1);
                var prod2 = session.Load<Product>(id2);
                Assert.IsType<MyClass>(prod2);
            }
        }
    }
    
    private abstract class Product
    {
        public string Name { get; set; }
    }
    private class CustomProduct : Product
    {

    }
    private class MyClass : CustomProduct
    {
        public string Arava => "omer";
    }
}
