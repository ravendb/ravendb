using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Indexes
{
    public class WithDecimalValue : RavenTestBase
    {
        public WithDecimalValue(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public decimal Value { get; set; }
        }

        private class Dec : AbstractIndexCreationTask<Item>
        {
            public Dec()
            {
                Map = items => from item in items
                               select new { A = item.Value * 0.83M };
            }
        }

        [Fact]
        public void CanCreate()
        {
            using (var store = GetDocumentStore())
            {
                new Dec().Execute(store);
            }
        }

        [Fact]
        public void IgnoresLocale()
        {
            using (CultureHelper.EnsureCulture(new CultureInfo("de")))
            {
                using (var store = GetDocumentStore())
                {
                    new Dec().Execute(store);
                }
            }
        }
    }
}
