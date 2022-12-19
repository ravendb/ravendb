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
