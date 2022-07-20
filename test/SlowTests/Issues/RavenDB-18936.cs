using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18936 : RavenTestBase
{
    public RavenDB_18936(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public Attribute[] Attributes { get; set; }
    }

    private class Attribute
    {
        public Attribute(string name, decimal value)
        {
            Name = name;
            Value = value;
        }

        public string Name { get; set; }
        public decimal Value { get; set; }
    }

    [Fact]
    public void CanCreateDynamicFieldsInsideMapJsIndex()
    {
        using var store = GetDocumentStore();

        var index = new CreateFieldInsideMap_JavaScript();
        index.Execute(store);

        using (var s = store.OpenSession())
        {
            s.Store(new Item {Attributes = new[]
            {
                new Attribute("T1", 10.99m), 
                new Attribute("T2", 12.99m), 
                new Attribute("T3", 13.99m)
            }});
            
            s.Store(new Item {Attributes = new[]
            {
                new Attribute("T1", 11.99m)
            }});
            
            s.SaveChanges();
        }

        using (var s = store.OpenSession())
        {
            var items = s.Advanced.DocumentQuery<Item, CreateFieldInsideMap_JavaScript>()
                .WaitForNonStaleResults()
                .OrderByDescending("T1")
                .ToList();
            Assert.Equal(2, items.Count);
            AssertTerm("T1", new[] {"10.99", "11.99"});
            AssertTerm("T2", new[] {"12.99"});
            AssertTerm("T3", new[] {"13.99"});
        }


        void AssertTerm(string fieldName, string[] termsThatShouldBeStored)
        {
            var terms = store
                .Maintenance
                .Send(new GetTermsOperation(index.IndexName, fieldName, null, 128));
            Assert.Equal(termsThatShouldBeStored.Length, terms.Length);
            foreach (var term in termsThatShouldBeStored)
            {
                Assert.Contains(term, terms);
            }
        }
    }

    private class CreateFieldInsideMap_JavaScript : AbstractJavaScriptIndexCreationTask
    {
        public CreateFieldInsideMap_JavaScript()
        {
            Maps = new HashSet<string>
            {
                @"map('Items', function (p) {
return {
_: p.Attributes.map(x => createField(x.Name, x.Value, { 
       indexing: 'Exact',
       storage: true,
       termVector: null
   }))
};
})",
            };
        }
    }
}
