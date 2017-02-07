using System.Globalization;
using System.Linq;
using FastTests;
using Raven.NewClient.Client.Indexes;
using Raven.Server.Utils;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class WithDecimalValue : RavenNewTestBase
    {
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
