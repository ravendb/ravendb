using System.Globalization;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15019 : RavenTestBase
    {
        public RavenDB_15019(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseTryConvertInIndex()
        {
            using (var store = GetDocumentStore())
            {
                var index = new ConvertIndex();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        DblNullValue = 1.1,
                        DblValue = 2.1,
                        FltNullValue = 3.1f,
                        FltValue = 4.1f,
                        IntNullValue = 5,
                        IntValue = 6,
                        LngNullValue = 7,
                        LngValue = 8,
                        ObjValue = new Company { Name = "HR" },
                        StgValue = "str"
                    }, "items/1");

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.DblNullValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.True(double.Parse(terms[0], CultureInfo.InvariantCulture).AlmostEquals(1.1));

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.DblValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.True(double.Parse(terms[0], CultureInfo.InvariantCulture).AlmostEquals(2.1));

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.FltNullValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.True(float.Parse(terms[0], CultureInfo.InvariantCulture).AlmostEquals(3.1f));

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.FltValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.True(float.Parse(terms[0], CultureInfo.InvariantCulture).AlmostEquals(4.1f));

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.IntNullValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("5", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.IntValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("6", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.LngNullValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("7", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.LngValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("8", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.ObjValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("-1", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.StgValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("-1", terms[0]);
            }
        }

        private class ConvertIndex : AbstractIndexCreationTask<Item>
        {
            public ConvertIndex()
            {
                Map = items => from item in items
                               select new
                               {
                                   DblValue = TryConvert<double>(item.DblValue) ?? -1,
                                   DblNullValue = TryConvert<double>(item.DblNullValue) ?? -1,
                                   FltValue = TryConvert<float>(item.FltValue) ?? -1,
                                   FltNullValue = TryConvert<float>(item.FltNullValue) ?? -1,
                                   LngValue = TryConvert<long>(item.LngValue) ?? -1,
                                   LngNullValue = TryConvert<long>(item.LngNullValue) ?? -1,
                                   IntValue = TryConvert<int>(item.IntValue) ?? -1,
                                   IntNullValue = TryConvert<int>(item.IntNullValue) ?? -1,
                                   StgValue = TryConvert<double>(item.StgValue) ?? -1,
                                   ObjValue = TryConvert<long>(item.ObjValue) ?? -1
                               };
            }
        }

        private class Item
        {
            public double DblValue { get; set; }

            public double? DblNullValue { get; set; }

            public float FltValue { get; set; }

            public float? FltNullValue { get; set; }

            public long LngValue { get; set; }

            public long? LngNullValue { get; set; }

            public int IntValue { get; set; }

            public int? IntNullValue { get; set; }

            public string StgValue { get; set; }

            public Company ObjValue { get; set; }
        }
    }
}
