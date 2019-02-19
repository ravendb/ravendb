using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class InIssues : RavenTestBase
    {
        private class Item
        {
            public string Key { get; set; }
        }

        [Fact]
        public void CanQueryProperly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var keys = new[]
                    {
                        "catalog/domaines-barons-de-rothschild-lafite-2010-val-lours-cabernet-sauvignon-syrah-pays-doc",
                        "catalog/giesen-2010-riesling-marlborough",
                        "catalog/foris-2009-dry-gewurztraminer-rogue-valley",
                        "catalog/trefethen-2005-reserve-cabernet-sauvignon-oak-knoll",

                        "catalog/giesen-2010-the-brothers-sauvignon-blanc-marlborough",
                        "catalog/foris-2009-maple-ranch-pinot-noir-rogue-valley",
                        "catalog/domaines-barons-de-rothschild-lafite-2010-val-lours-chardonnay-pays-doc"
                    };

                    foreach (var key in keys)
                    {
                        session.Store(new Item { Key = key });
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var partialKeys = new[]
                    {
                        "catalog/domaines-barons-de-rothschild-lafite-2010-val-lours-cabernet-sauvignon-syrah-pays-doc",
                        "catalog/giesen-2010-riesling-marlborough",
                        "catalog/foris-2009-dry-gewurztraminer-rogue-valley",
                        "catalog/trefethen-2005-reserve-cabernet-sauvignon-oak-knoll",
                    };

                    var results = session.Query<Item>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Key.In(partialKeys))
                        .ToList();
                    foreach (var partialKey in partialKeys)
                    {
                        Assert.True(results.Any(x => x.Key == partialKey));
                    }
                }
            }
        }
    }
}
