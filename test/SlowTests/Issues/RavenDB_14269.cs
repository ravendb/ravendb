using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14269 : RavenTestBase
    {
        public RavenDB_14269(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public async Task JsConverterShouldNotStripValueOrKeyFromDictionaryEntity(string jsEngineType)
        {
            using var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType));
            using var session = store.OpenAsyncSession();

            var u1 = new User
            {
                Name = "Egor",
                Phones = new Dictionary<string, Phone>
                {
                    {"Home", new Phone {CountryPrefix = "0972", Value = "544543744"}}, {"Work", new Phone {CountryPrefix = "0380", Value = "501750112"}}
                }
            };
            await session.StoreAsync(u1, "user/1");

            await session.StoreAsync(new User
            {
                Name = "T",
                Phones = new Dictionary<string, Phone> {
                    {
                        "Home", new Phone { CountryPrefix= "0024", Value= "61234218" }
                    },
                    {
                        "Work", new Phone { CountryPrefix= "0034", Value= "6212678" }
                    }
                }
            }, "user/2");
            await session.StoreAsync(new User
            {
                Name = "A",
                Phones = new Dictionary<string, Phone> {
                    {
                        "Home", new Phone { CountryPrefix= "0024", Value= "61234218" }
                    },
                    {
                        "Work", new Phone { CountryPrefix= "0034", Value= "6212678" }
                    }
                }
            }, "user/3");
            await session.StoreAsync(new User
            {
                Name = "X",
                Phones = new Dictionary<string, Phone> {
                    {
                        "Home", new Phone{ CountryPrefix= "0024", Value= "61234218" }
                    },
                    {
                        "Work", new Phone { CountryPrefix= "0034", Value= "6212678" }
                    }
                }
            }, "user/4");

            var u5 = new User
            {
                Name = "gg",
                Phones = new Dictionary<string, Phone>
                {
                    {"Home", new Phone {CountryPrefix = "0024", Value = "61212318"}}, {"Work", new Phone {CountryPrefix = "0034", Value = "332678"}}
                }
            };
            await session.StoreAsync(u5, "user/5");

            await session.SaveChangesAsync();

            var query =
                from customer in session.Query<User>()
                select new PhoneListModel
                {
                    CustomerName = customer.Name,
                    Phones =
                        from phone in customer.Phones
                        select new PhoneModel
                        {
                            Label = phone.Key,
                            Prefix = phone.Value.CountryPrefix,
                            Phone = phone.Value.Value
                        }
                };

            Assert.Equal("from 'Users' as customer select { CustomerName : customer?.Name, Phones : Object.map(customer?.Phones, function(v, k){ return {Label:k,Prefix:v?.CountryPrefix,Phone:v?.Value};}) }", query.ToString());

            var res = await query.ToListAsync();
            Assert.Equal(5, res.Count);
            Assert.Equal(res.First().CustomerName, u1.Name);
            Assert.Equal(2, res.First().Phones.Count());
            Assert.Equal(res.First().Phones.First().Label, u1.Phones.First().Key);
            Assert.Equal(res.First().Phones.First().Phone, u1.Phones.First().Value.Value);
            Assert.Equal(res.First().Phones.First().Prefix, u1.Phones.First().Value.CountryPrefix);
            Assert.Equal(res.First().Phones.Last().Label, u1.Phones.Last().Key);
            Assert.Equal(res.First().Phones.Last().Phone, u1.Phones.Last().Value.Value);
            Assert.Equal(res.First().Phones.Last().Prefix, u1.Phones.Last().Value.CountryPrefix);


            Assert.Equal(res.Last().CustomerName, u5.Name);
            Assert.Equal(2, res.Last().Phones.Count());
            Assert.Equal(res.Last().Phones.First().Label, u5.Phones.First().Key);
            Assert.Equal(res.Last().Phones.First().Phone, u5.Phones.First().Value.Value);
            Assert.Equal(res.Last().Phones.First().Prefix, u5.Phones.First().Value.CountryPrefix);
            Assert.Equal(res.Last().Phones.Last().Label, u5.Phones.Last().Key);
            Assert.Equal(res.Last().Phones.Last().Phone, u5.Phones.Last().Value.Value);
            Assert.Equal(res.Last().Phones.Last().Prefix, u5.Phones.Last().Value.CountryPrefix);
        }

        private class User
        {
            public string Name { get; set; }
            public Dictionary<string, Phone> Phones { get; set; }
        }

        private class PhoneListModel
        {
            public string CustomerName { get; set; }
            public IEnumerable<PhoneModel> Phones { get; set; }
        }

        private class PhoneModel
        {
            public string Label { get; set; }
            public string Prefix { get; set; }
            public string Phone { get; set; }
        }

        private class Phone
        {
            public string CountryPrefix { get; set; }
            public string Value { get; set; }
        }
    }
}
