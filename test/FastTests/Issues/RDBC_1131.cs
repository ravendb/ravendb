using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RDBC_1131 : RavenTestBase
    {
        private const string Id = "users/1";

        public RDBC_1131(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Load_PopulatesExtensionDictionary()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using var session = store.OpenAsyncSession();
            var user = await session.LoadAsync<User>(Id);

            Assert.Equal("Liel", user.Name);
            Assert.NotNull(user.Extra);
            Assert.Equal(new[] { "@metadata", "Age", "City" }, user.Extra.Keys.OrderBy(x => x, StringComparer.Ordinal));
            Assert.Equal(30, Convert.ToInt32(user.Extra["Age"]));
            Assert.Equal("Hadera", user.Extra["City"].ToString());
            Assert.Equal(Id, ((JObject)user.Extra["@metadata"])["@id"]?.Value<string>());
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Loaded_EditMetadataInDictionary_IsNotWritten()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(Id);
                var metadata = (JObject)user.Extra["@metadata"];
                metadata["@collection"] = "Hacked";
                metadata["Fake"] = "value";
                user.Name = "Liel Nagar";
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("Liel Nagar", Get<string>(raw, "Name"));
            Assert.Equal(30, Get<long>(raw, "Age"));
            Assert.Equal("Hadera", Get<string>(raw, "City"));
            var rawMetadata = (JObject)raw["@metadata"];
            Assert.Equal("Users", Get<string>(rawMetadata, "@collection"));
            Assert.False(rawMetadata.ContainsKey("Fake"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Loaded_ModifyIdThroughDictionary_IdIsUnchanged()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(Id);
                user.Extra["Id"] = "users/2";
                user.Extra["@id"] = "users/2";
                ((JObject)user.Extra["@metadata"])["@id"] = "users/2";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(Id);
                Assert.NotNull(user);
                Assert.Equal(Id, user.Id);
                Assert.Equal(Id, session.Advanced.GetDocumentId(user));
                Assert.Null(await session.LoadAsync<User>("users/2"));
            }

            var raw = await GetRawAsync(store, Id);
            Assert.DoesNotContain("Id", PropertyNames(raw));
            Assert.DoesNotContain("@id", PropertyNames(raw));
            Assert.Equal(Id, Get<string>((JObject)raw["@metadata"], "@id"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Store_IgnoredIdentityProperty_IdKeyInDictionaryIsNotWritten()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                // the identity property is ignored, so it is never matched as a property and the dictionary key must still not reach the document
                await session.StoreAsync(new IgnoredIdUser
                {
                    Name = "Liel",
                    Extra = new Dictionary<string, object> { ["Id"] = "users/2", ["Phone"] = "050-1234" }
                }, Id);
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.DoesNotContain("Id", PropertyNames(raw));
            Assert.Equal("050-1234", Get<string>(raw, "Phone"));

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<IgnoredIdUser>(Id);
                Assert.Equal(Id, session.Advanced.GetDocumentId(user));
                Assert.Null(await session.LoadAsync<IgnoredIdUser>("users/2"));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Loaded_DictionaryKeyMatchesProperty_PropertyWins()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new AddressUser { Name = "Liel", Address = "Hadera", Extra = new Dictionary<string, object>() }, Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<AddressUser>(Id);
                user.Extra["Address"] = "Tel-Aviv";
                user.Extra["address"] = "Tel-Aviv";
                user.Extra["id"] = "users/2";
                user.Extra["Phone"] = "050-1234";
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("Hadera", Get<string>(raw, "Address"));
            Assert.Equal("050-1234", Get<string>(raw, "Phone"));
            Assert.DoesNotContain("address", PropertyNames(raw));
            Assert.DoesNotContain("id", PropertyNames(raw));

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<AddressUser>(Id);
                Assert.Equal("Hadera", user.Address);
                Assert.Equal(Id, user.Id);
                Assert.False(user.Extra.ContainsKey("Address"));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Store_DictionaryKeyMatchesIgnoredProperty_IsWritten()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new AddressUser
                {
                    Name = "Liel",
                    Secret = "from-property",
                    Extra = new Dictionary<string, object> { ["Secret"] = "from-dictionary" }
                }, Id);
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("from-dictionary", Get<string>(raw, "Secret"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Inheritance_BaseClassDictionary_KeysRoundTripUnchanged()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Employee
                {
                    Name = "Liel",
                    Company = "RavenDB",
                    Extra = new Dictionary<string, object>
                    {
                        ["Age"] = 30,
                        ["City"] = "Hadera",
                        ["Address"] = new Dictionary<string, object> { ["Street"] = "Herzel" }
                    }
                }, Id);
                await session.SaveChangesAsync();
            }

            var stored = await GetRawAsync(store, Id);
            Assert.Equal("RavenDB", Get<string>(stored, "Company"));

            using (var session = store.OpenAsyncSession())
            {
                // loaded through the base type, the Raven-Clr-Type still materializes the derived entity
                var user = await session.LoadAsync<User>(Id);
                var employee = Assert.IsType<Employee>(user);
                Assert.Equal("RavenDB", employee.Company);
                Assert.Equal(new[] { "@metadata", "Address", "Age", "City" }, employee.Extra.Keys.OrderBy(x => x, StringComparer.Ordinal));
                Assert.False(session.Advanced.HasChanged(employee));

                employee.Name = "Liel Nagar";
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("Liel Nagar", Get<string>(raw, "Name"));
            foreach (var key in new[] { "Age", "City", "Address", "Company" })
                Assert.True(JToken.DeepEquals(stored[key], raw[key]), $"Property '{key}' was modified");
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Inheritance_DictionaryKeyMatchesDerivedProperty_PropertyWins()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Employee { Name = "Liel", Company = "RavenDB", Extra = new Dictionary<string, object>() }, Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var employee = await session.LoadAsync<Employee>(Id);
                employee.Extra["Company"] = "Hacked";
                employee.Extra["Name"] = "Hacked";
                employee.Extra["Phone"] = "050-1234";
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("RavenDB", Get<string>(raw, "Company"));
            Assert.Equal("Liel", Get<string>(raw, "Name"));
            Assert.Equal("050-1234", Get<string>(raw, "Phone"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Loaded_BagOnlyEdit_HasChanged()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using var session = store.OpenAsyncSession();
            var user = await session.LoadAsync<User>(Id);

            Assert.False(session.Advanced.HasChanged(user));
            Assert.False(session.Advanced.HasChanges);

            user.Extra["City"] = "Tel Aviv";

            Assert.True(session.Advanced.HasChanged(user));
            Assert.True(session.Advanced.HasChanges);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task JTokenDictionary_LoadEditAndStore_IsPersisted()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<JTokenUser>(Id);
                Assert.Equal(30, user.Extra["Age"].Value<int>());
                user.Extra["City"] = "Tel Aviv";
                user.Extra["Address"] = new JObject { ["Street"] = "Herzel" };
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal(30, Get<long>(raw, "Age"));
            Assert.Equal("Tel Aviv", Get<string>(raw, "City"));
            Assert.Equal("Herzel", Get<string>((JObject)raw["Address"], "Street"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task PreservePropertiesConventionOff_DictionaryStillRoundTrips()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.PreserveDocumentPropertiesNotFoundOnModel = false
            });
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(Id);
                user.Extra["Phone"] = "050-1234";
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("050-1234", Get<string>(raw, "Phone"));
            Assert.Equal(30, Get<long>(raw, "Age"));
            Assert.Equal("Hadera", Get<string>(raw, "City"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task WriteDataFalse_KeepsUnknownFields()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<WriteDataFalseUser>(Id);
                Assert.Equal(30, Convert.ToInt32(user.Extra["Age"]));
                user.Name = "Liel Nagar";
                user.Extra["Phone"] = "050-1234";
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("Liel Nagar", Get<string>(raw, "Name"));
            Assert.Equal(30, Get<long>(raw, "Age"));
            Assert.Equal("Hadera", Get<string>(raw, "City"));
            Assert.DoesNotContain("Phone", PropertyNames(raw));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task ReadDataFalse_KeepsUnknownFieldsAndWritesDictionary()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<ReadDataFalseUser>(Id);
                Assert.Null(user.Extra);
                user.Name = "Liel Nagar";
                user.Extra = new Dictionary<string, object> { ["Phone"] = "050-1234" };
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("Liel Nagar", Get<string>(raw, "Name"));
            Assert.Equal(30, Get<long>(raw, "Age"));
            Assert.Equal("Hadera", Get<string>(raw, "City"));
            Assert.Equal("050-1234", Get<string>(raw, "Phone"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task ReadDataFalse_DictionaryKeyMatchesUnknownField_DictionaryWins()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<ReadDataFalseUser>(Id);
                user.Extra = new Dictionary<string, object> { ["Age"] = 31 };
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal(31, Get<long>(raw, "Age"));
            Assert.Single(PropertyNames(raw), x => x == "Age");
            Assert.Equal("Hadera", Get<string>(raw, "City"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Store_NewEntity_WritesExtensionData()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "Liel",
                    Extra = new Dictionary<string, object>
                    {
                        ["Age"] = 30,
                        ["Address"] = new Dictionary<string, object> { ["Street"] = "Herzel" },
                        ["Tags"] = new[] { "a", "b" },
                        ["Nothing"] = null
                    }
                }, Id);
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("Liel", Get<string>(raw, "Name"));
            Assert.Equal(30, Get<long>(raw, "Age"));
            Assert.True(raw.TryGetValue("Address", out var address), "Property 'Address' is missing");
            Assert.Equal("Herzel", Get<string>((JObject)address, "Street"));
            Assert.True(raw.TryGetValue("Tags", out var tags), "Property 'Tags' is missing");
            Assert.Equal(new[] { "a", "b" }, tags.Values<string>());
            Assert.True(raw.TryGetValue("Nothing", out var nothing), "Property 'Nothing' is missing");
            Assert.Equal(JTokenType.Null, nothing.Type);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Write_MetadataKeyInDictionary_IsIgnored()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "Liel",
                    Extra = new Dictionary<string, object>
                    {
                        ["Age"] = 30,
                        ["@metadata"] = new Dictionary<string, object> { ["@collection"] = "Hacked", ["Fake"] = "value" }
                    }
                }, Id);
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal(30, Get<long>(raw, "Age"));

            var metadata = (JObject)raw["@metadata"];
            Assert.Equal("Users", Get<string>(metadata, "@collection"));
            Assert.True(metadata.ContainsKey("Raven-Clr-Type"));
            Assert.False(metadata.ContainsKey("Fake"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Loaded_ReplaceDictionary_IsPersisted()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(Id);
                user.Extra = new Dictionary<string, object> { ["Phone"] = "050-1234" };
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("050-1234", Get<string>(raw, "Phone"));
            Assert.DoesNotContain("Age", PropertyNames(raw));
            Assert.DoesNotContain("City", PropertyNames(raw));
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Loaded_ClearOrNullDictionary_RemovesUnknownFields(bool setNull)
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(Id);
                if (setNull)
                    user.Extra = null;
                else
                    user.Extra.Clear();
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.DoesNotContain("Age", PropertyNames(raw));
            Assert.DoesNotContain("City", PropertyNames(raw));
            Assert.Equal("Liel", Get<string>(raw, "Name"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Loaded_BagEditPlusKnownPropertyChange_BothPersisted()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(Id);
                user.Name = "Liel Nagar";
                user.Extra["City"] = "Tel Aviv";
                user.Extra.Remove("Age");
                user.Extra["Phone"] = "050-1234";
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("Liel Nagar", Get<string>(raw, "Name"));
            Assert.Equal("Tel Aviv", Get<string>(raw, "City"));
            Assert.Equal("050-1234", Get<string>(raw, "Phone"));
            Assert.DoesNotContain("Age", PropertyNames(raw));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Stream_ForwardBagToNewObject_BulkInsert_PreservesFields()
        {
            using var store = GetDocumentStore();
            await SeedAsync(store, Id);

            User source;
            using (var session = store.OpenAsyncSession())
            {
                await using var stream = await session.Advanced.StreamAsync<User>("users/");
                Assert.True(await stream.MoveNextAsync());
                source = stream.Current.Document;
            }

            Assert.True(source.Extra.ContainsKey("Age"));

            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new User
                {
                    Name = "Liel Nagar",
                    Extra = source.Extra
                }, Id);
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("Liel Nagar", Get<string>(raw, "Name"));
            Assert.Equal(30, Get<long>(raw, "Age"));
            Assert.Equal("Hadera", Get<string>(raw, "City"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Nested_ExtensionData_RoundTripsAndIsPersisted()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order { Shipping = new ShippingAddress { City = "Hadera" } }, Id);
                await session.SaveChangesAsync();
            }

            await store.Operations.SendAsync(new PatchOperation(Id, null, new PatchRequest { Script = "this.Shipping.Zip = '38000';" }));

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>(Id);
                Assert.Equal(new[] { "Zip" }, order.Shipping.Extra.Keys);
                Assert.False(session.Advanced.HasChanged(order));

                order.Shipping.Extra["Floor"] = 3;
                order.Shipping.Extra["city"] = "Hacked";
                await session.SaveChangesAsync();
            }

            var shipping = (JObject)(await GetRawAsync(store, Id))["Shipping"];
            Assert.Equal("Hadera", Get<string>(shipping, "City"));
            Assert.Equal("38000", Get<string>(shipping, "Zip"));
            Assert.Equal(3, Get<long>(shipping, "Floor"));
            Assert.DoesNotContain("city", PropertyNames(shipping));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CamelCaseNaming_DictionaryKeyMatchesProperty_PropertyWins()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    var serialization = new NewtonsoftJsonSerializationConventions();
                    ((DefaultContractResolver)serialization.JsonContractResolver).NamingStrategy = new CamelCaseNamingStrategy();
                    s.Conventions.Serialization = serialization;
                    s.Conventions.PropertyNameConverter = mi => char.ToLowerInvariant(mi.Name[0]) + mi.Name.Substring(1);
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "Liel",
                    Extra = new Dictionary<string, object> { ["Name"] = "Hacked", ["Id"] = "users/2", ["Phone"] = "050-1234" }
                }, Id);
                await session.SaveChangesAsync();
            }

            var raw = await GetRawAsync(store, Id);
            Assert.Equal("Liel", Get<string>(raw, "name"));
            Assert.Equal("050-1234", Get<string>(raw, "Phone"));
            Assert.DoesNotContain("Name", PropertyNames(raw));
            Assert.DoesNotContain("Id", PropertyNames(raw));
            Assert.DoesNotContain("id", PropertyNames(raw));

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(Id);
                Assert.Equal("Liel", user.Name);
                Assert.Equal(Id, session.Advanced.GetDocumentId(user));
            }
        }

        private static async Task SeedAsync(IDocumentStore store, string id)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Liel" }, id);
                await session.SaveChangesAsync();
            }

            // seed the unknown fields with a patch, so the setup doesn't depend on the write path under test
            await store.Operations.SendAsync(new PatchOperation(id, null, new PatchRequest { Script = "this.Age = 30; this.City = 'Hadera';" }));
        }

        // reads the document as stored on the server, bypassing the entity serialization under test
        private static async Task<JObject> GetRawAsync(IDocumentStore store, string id)
        {
            using var commands = store.Commands();
            var document = await commands.GetAsync(id);
            Assert.NotNull(document);
            return JObject.Parse(document.BlittableJson.ToString());
        }

        private static T Get<T>(JObject json, string name)
        {
            Assert.True(json.TryGetValue(name, out var value), $"Property '{name}' is missing");
            return value.Value<T>();
        }

        private static IEnumerable<string> PropertyNames(JObject json) => json.Properties().Select(x => x.Name);

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }

            [JsonExtensionData]
            public IDictionary<string, object> Extra { get; set; }
        }

        private class IgnoredIdUser
        {
            [JsonIgnore]
            public string Id { get; set; }
            public string Name { get; set; }

            [JsonExtensionData]
            public IDictionary<string, object> Extra { get; set; }
        }

        private class AddressUser : User
        {
            public string Address { get; set; }

            [JsonIgnore]
            public string Secret { get; set; }
        }

        private class Employee : User
        {
            public string Company { get; set; }
        }

        private class JTokenUser
        {
            public string Id { get; set; }
            public string Name { get; set; }

            [JsonExtensionData]
            public IDictionary<string, JToken> Extra { get; set; }
        }

        private class WriteDataFalseUser
        {
            public string Id { get; set; }
            public string Name { get; set; }

            [JsonExtensionData(WriteData = false)]
            public IDictionary<string, object> Extra { get; set; }
        }

        private class ReadDataFalseUser
        {
            public string Id { get; set; }
            public string Name { get; set; }

            [JsonExtensionData(ReadData = false)]
            public IDictionary<string, object> Extra { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public ShippingAddress Shipping { get; set; }
        }

        private class ShippingAddress
        {
            public string City { get; set; }

            [JsonExtensionData]
            public IDictionary<string, object> Extra { get; set; }
        }
    }
}
