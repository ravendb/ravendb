// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2994.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2994 : RavenTestBase
    {
        public RavenDB_2994(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Id { get; set; }

            public string ValueAsInt { get; set; }

            public string ValueAsDouble { get; set; }

            public string ValueAsDecimal { get; set; }

            public string ValueAsShort { get; set; }

            public string ValueAsLong { get; set; }
        }

        private class Items_Numbers : AbstractIndexCreationTask<Item>
        {
            public class Result
            {
                public string Id { get; set; }

                public int Int1 { get; set; }

                public int Int2 { get; set; }

                public double Double1 { get; set; }

                public double Double2 { get; set; }

                public decimal Decimal1 { get; set; }

                public decimal Decimal2 { get; set; }

                public long Long1 { get; set; }

                public long Long2 { get; set; }
            }

            public Items_Numbers()
            {
                Map = items => from item in items
                               select new
                               {
                                   Int1 = item.ValueAsInt.ParseInt(),
                                   Int2 = item.ValueAsInt.ParseInt(-1),
                                   Double1 = item.ValueAsDouble.ParseDouble(),
                                   Double2 = item.ValueAsDouble.ParseDouble(-1),
                                   Decimal1 = item.ValueAsDecimal.ParseDecimal(),
                                   Decimal2 = item.ValueAsDecimal.ParseDecimal(-1),
                                   Long1 = item.ValueAsLong.ParseLong(),
                                   Long2 = item.ValueAsLong.ParseLong(-1),
                                   Id = item.Id
                               };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void PraseIndexingExtensionsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new Items_Numbers().Execute(store);

                decimal dec = 11.5m;
                double dbl = 10.2;
                short s = 52;
                int i = 2;
                long l = long.MaxValue;

                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        ValueAsDecimal = dec.ToString(CultureInfo.InvariantCulture),
                        ValueAsDouble = dbl.ToString(CultureInfo.InvariantCulture),
                        ValueAsInt = i.ToString(CultureInfo.InvariantCulture),
                        ValueAsShort = s.ToString(CultureInfo.InvariantCulture),
                        ValueAsLong = l.ToString(CultureInfo.InvariantCulture)
                    });

                    session.Store(new Item
                    {
                        ValueAsDecimal = "one",
                        ValueAsDouble = "one",
                        ValueAsInt = "one",
                        ValueAsShort = "one",
                        ValueAsLong = "one"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var items = session
                        .Query<Items_Numbers.Result, Items_Numbers>()
                        .ProjectInto<Items_Numbers.Result>()
                        .ToList();

                    Assert.Equal(2, items.Count);

                    var item1 = items.Single(x => x.Id == "items/1-A");

                    Assert.Equal(dec, item1.Decimal1);
                    Assert.Equal(dec, item1.Decimal2);

                    Assert.Equal(dbl, item1.Double1);
                    Assert.Equal(dbl, item1.Double2);

                    Assert.Equal(i, item1.Int1);
                    Assert.Equal(i, item1.Int2);

                    Assert.Equal(l, item1.Long1);
                    Assert.Equal(l, item1.Long2);

                    var item2 = items.Single(x => x.Id == "items/2-A");

                    Assert.Equal(default(decimal), item2.Decimal1);
                    Assert.Equal(-1, item2.Decimal2);

                    Assert.Equal(default(double), item2.Double1);
                    Assert.Equal(-1, item2.Double2);

                    Assert.Equal(default(int), item2.Int1);
                    Assert.Equal(-1, item2.Int2);

                    Assert.Equal(default(long), item2.Long1);
                    Assert.Equal(-1, item2.Long2);
                }
            }
        }
    }
}
