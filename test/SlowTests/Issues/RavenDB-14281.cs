using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14281 : RavenTestBase
    {
        public RavenDB_14281(ITestOutputHelper output) : base(output)
        {
        }


        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task Can_Generate_Correct_Javascript_Projection_for_IDictionary(Options options)
        {
            using (var store = GetDocumentStore(options))
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(
                    new User
                    {
                        GivenName = "X",
                        FamilyName = "Y",
                        Phones = { { "Home", new Phone("0024", "61234218") }, { "Work", new Phone("0034", "6212678") } },
                        Phones2 = { { "Home", new Phone("0024", "61234218") }, { "Work", new Phone("0034", "6212678") } }
                    }, "user/4");

                await session.SaveChangesAsync();

                var query =
                    from customer in session.Query<User>()
                    select new
                    {
                        CustomerName = customer.GivenName + " " + customer.FamilyName,
                        Phone =
                            from phone in customer.Phones
                            where phone.Key == "Work"
                            select phone
                    };

                var q1 = query.ToString();
                var expected =
                    "from 'Users' as customer select { CustomerName : customer?.GivenName+\" \"+customer?.FamilyName, " +
                    "Phone : (((Object.keys(customer?.Phones)?.map(function(a){return{Key: a,Value:customer?.Phones[a]};}))??[]).filter(function(phone){return phone?.Key===\"Work\";})) }";
                Assert.Equal(expected, q1);

                var query2 =
                    from customer in session.Query<User>()
                    select new
                    {
                        CustomerName = customer.GivenName + " " + customer.FamilyName,
                        Phone =
                            from phone in customer.Phones2
                            where phone.Key == "Work"
                            select phone
                    };


                var q2 = query2.ToString();
                expected =
                    "from 'Users' as customer select { CustomerName : customer?.GivenName+\" \"+customer?.FamilyName, " +
                    "Phone : (((Object.keys(customer?.Phones2)?.map(function(a){return{Key: a,Value:customer?.Phones2[a]};}))??[]).filter(function(phone){return phone?.Key===\"Work\";})) }";
                Assert.Equal(expected, q2);

                var res1 = await query.ToArrayAsync();
                var res2 = await query2.ToArrayAsync();

                Assert.Equal(res1.Length, res2.Length);
                Assert.Equal(res1[0].CustomerName, res2[0].CustomerName);

                var phones1 = res1[0].Phone.ToList();
                var keys1 = phones1.Select(x => x.Key);
                var phones2 = res2[0].Phone.ToList();
                var keys2 = phones2.Select(x => x.Key);
                Assert.True(keys1.SequenceEqual(keys2));

                var values1 = phones1.Select(x => x.Value.Value);
                var values2 = phones2.Select(x => x.Value.Value);
                Assert.True(values1.SequenceEqual(values2));

                var countryPrefix1 = phones1.Select(x => x.Value.CountryPrefix);
                var countryPrefix2 = phones2.Select(x => x.Value.CountryPrefix);
                Assert.True(countryPrefix1.SequenceEqual(countryPrefix2));
            }
        }

        private class User
        {
            public string GivenName { get; set; }

            public string FamilyName { get; set; }

            public IDictionary<string, Phone> Phones { get; } = new Dictionary<string, Phone>();

            public Dictionary<string, Phone> Phones2 { get; } = new Dictionary<string, Phone>();
        }

        private class Phone
        {
            public Phone(string prefix, string value)
            {
                CountryPrefix = prefix;
                Value = value;
            }

            public string CountryPrefix { get; set; }
            public string Value { get; set; }
        }

        private class DictionaryString<T> : Dictionary<string, T>
        {
            public DictionaryString()
                : base(StringComparer.OrdinalIgnoreCase)
            {

            }
        }

        private class Invoice
        {
            public class VAT
            {
                public decimal Price { get; set; }
                public string VATRateId { get; set; }
                public string VATCode { get; set; }
                public decimal Percentage { get; set; }
            }

            public string Id { get; set; }

            public DictionaryString<VAT> VATTotals { get; set; }
        }

        private class VATRate
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class VATTotalProjection
        {
            public decimal Price { get; internal set; }
            public decimal Percentage { get; internal set; }
            public string VATName { get; internal set; }
        }

        [Fact]
        public async Task BoolOrderTest()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Invoice()
                    {
                        VATTotals = new DictionaryString<Invoice.VAT>()
                        {
                            {"vatrates/1", new Invoice.VAT() {Price = 100, VATRateId = "vatrates/1", VATCode = "VAT Low", Percentage = 9}},
                            {"vatrates/2", new Invoice.VAT() {Price = 100, VATRateId = "vatrates/2", VATCode = "VAT High", Percentage = 21}}
                        }
                    });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from invoice in session.Query<Invoice>()
                                let vat = invoice.VATTotals
                                select new
                                {
                                    Id = invoice.Id,
                                    VATTotals = vat.Select(b => new VATTotalProjection()
                                    {
                                        Price = b.Value.Price,
                                        Percentage = b.Value.Percentage,
                                        VATName = RavenQuery.Load<VATRate>(b.Value.VATRateId).Name
                                    })
                                };


                    var result = await query.ToListAsync();
                    Assert.Equal(1, result.Count);
                }
            }
        }
    }
}
