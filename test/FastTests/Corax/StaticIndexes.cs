using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Lucene.Net.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class StaticIndexes : RavenTestBase
{
    public StaticIndexes(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void BoostInIndexDefinitionException(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var exception = Assert.Throws<IndexCreationException>(() =>
                store.Maintenance.Send(new PutIndexesOperation(new[]
                {
                    new IndexDefinition {Name = "test", Maps = {"from p in docs.Products select new { p.Price} .Boost(2)"}}
                })));
            Assert.True(exception.Message.Contains($"{nameof(Corax)} is not supporting boosting inside index yet. Please use Lucene engine."));
        }
    }
    
    private class Product
    {
        public string Id { get; set; }
        public List<Attribute> Attributes { get; set; }
    }

    private class Attribute
    {
        public string Name { get; set; }
        public string Value { get; set; }
        public decimal NumericValue { get; set; }
        public int IntValue { get; set; }
    }

    private class Product_ByAttribute : AbstractIndexCreationTask<Product>
    {
        public Product_ByAttribute()
        {
            Map = products =>
                from p in products
                select new {_ = p.Attributes.Select(attribute => new Field(attribute.Name, attribute.Value, Field.Store.NO, Field.Index.ANALYZED))};
        }
    }
}
