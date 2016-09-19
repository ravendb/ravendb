using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Raven.Server.Utils;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class WithDecimalValue : RavenTestBase
    {
        public class Item
        {
            public decimal Value { get; set; }
        }

        public class Dec : AbstractIndexCreationTask<Item>
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
