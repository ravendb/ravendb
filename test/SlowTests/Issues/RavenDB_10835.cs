using System;
using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10835 : RavenTestBase
    {
        public RavenDB_10835(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Nullable_DateTime_And_DynamicNullObject_Conversion()
        {
            using (var store = GetDocumentStore())
            {
                new Index_test().Execute(store);
            }
        }

        private class Index_test : AbstractMultiMapIndexCreationTask<Result>
        {
            public Index_test()
            {
                AddMap<Item>(items => from item in items
                                      select new
                                      {
                                          Id = item.Id,
                                          CreatedAt = item.CreatedAt != null
                                              ? (DateTime?)DateTime.ParseExact(item.CreatedAt,
                                                  "yyyy-MM-dd",
                                                  CultureInfo.InvariantCulture, DateTimeStyles.None)
                                              : null
                                      });
            }
        }

        private class Item
        {
            public string Id { get; set; }
            public string CreatedAt { get; set; }
        }

        private class Result
        {
            public string Id { get; set; }
            public DateTime? CreatedAt { get; set; }
        }
    }
}
