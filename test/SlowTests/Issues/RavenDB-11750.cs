using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using NodaTime;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11750 : RavenTestBase
    {
        public static class JsonConverterExtensions
        {
            public class InstantConverter : JsonConverter<Instant>
            {
                public override Instant ReadJson(JsonReader reader, Type type, Instant existingValue, Boolean hasExistingValue, JsonSerializer serializer)
                {
                    DateTime value = serializer.Deserialize<DateTime>(reader);
                    return Instant.FromDateTimeUtc(value);
                    //string timestamp = serializer.Deserialize<string>(reader);
                    //InstantPattern pattern = InstantPattern.CreateWithInvariantCulture("yyyy-MM-ddTHH:mm:ss.fffffff'Z'");
                    //return pattern.Parse(timestamp).Value;
                }

                public override void WriteJson(JsonWriter writer, Instant instant, JsonSerializer serializer)
                {
                    DateTime value = instant.ToDateTimeUtc();
                    serializer.Serialize(writer, value);
                    //InstantPattern pattern = InstantPattern.CreateWithInvariantCulture("yyyy-MM-ddTHH:mm:ss.fffffff'Z'");
                    //string timestamp = pattern.Format(instant);
                    //serializer.Serialize(writer, timestamp);
                }
            }
        }

        public static class JsonExtensions
        {
            /// <summary>
            /// 
            /// </summary>
            public static T FromJson<T>(object value)
            {
                return JsonConvert.DeserializeObject<T>(value as string, new JsonSerializerSettings
                {
                    Converters = new List<JsonConverter>
                {
                    new JsonConverterExtensions.InstantConverter()
                },

                    DateParseHandling = DateParseHandling.None,
                    DateTimeZoneHandling = DateTimeZoneHandling.Utc
                });
            }

            public static string ToJson(object value)
            {
                return JsonConvert.SerializeObject(value, new JsonSerializerSettings
                {
                    Converters = new List<JsonConverter>
                {
                    new JsonConverterExtensions.InstantConverter()
                },

                    DateParseHandling = DateParseHandling.None,
                    DateTimeZoneHandling = DateTimeZoneHandling.Utc
                });
            }
        }
        public static bool InstantQueryValueConverter(string name, Instant instant, Boolean range, out string outputValue)
        {
            outputValue = JsonExtensions.ToJson(instant.ToDateTimeUtc());
            //InstantPattern pattern = InstantPattern.CreateWithInvariantCulture("yyyy-MM-ddTHH:mm:ss.fffffff'Z'");
            //string timestamp = pattern.Format(instant);
            //outputValue = timestamp;
            return true;
        }

        public static void ModifyDocumentStore(IDocumentStore documentStore)
        {
            Action<JsonSerializer> previousCustomSerializer = documentStore.Conventions.CustomizeJsonSerializer;
            documentStore.Conventions.CustomizeJsonSerializer = (serializer =>
            {
                previousCustomSerializer?.Invoke(serializer);

                serializer.Converters.Add(new JsonConverterExtensions.InstantConverter());
                serializer.DateParseHandling = DateParseHandling.None;
                serializer.DateTimeZoneHandling = DateTimeZoneHandling.Utc;
            });

            documentStore.Conventions.RegisterQueryValueConverter<Instant>(InstantQueryValueConverter);
        }

        [Fact]
        public async Task CanUsePatchWithNodaTime()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = ModifyDocumentStore
            }))
            {
                await CreateUserAsync(store, "mark");
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    User user = await session.LoadAsync<User>("users/mark");
                    //user.UpdateTime = Time.GetCurrentTime();
                    //await session.SaveChangesAsync();
                    session.Advanced.Patch<User, Instant>("users/mark", x => x.UpdateTime, Time.GetCurrentTime());
                    await session.SaveChangesAsync();
                }

            }
        }

        private static async Task CreateUserAsync(IDocumentStore store, string userName)
        {
            using (IAsyncDocumentSession session = store.OpenAsyncSession())
            {
                string key = String.Format("users/{0}", userName);
                Boolean userExists = await session.Advanced.ExistsAsync(key);
                if (!userExists)
                {
                    await session.StoreAsync(new User
                    {
                        Id = key,
                        UserName = "mark",
                        CreateTime = Time.GetCurrentTime(),
                        UpdateTime = Time.GetCurrentTime()
                    });

                    await session.SaveChangesAsync();
                }
            }
        }
        private class User
        {
            private string id;

            public string Id
            {
                get
                {
                    return id;
                }

                set
                {
                    id = value;
                }
            }

            private string userName;

            public string UserName
            {
                get
                {
                    return userName;
                }

                set
                {
                    userName = value;
                }
            }

            private Instant createTime;

            public Instant CreateTime
            {
                get
                {
                    return createTime;
                }

                set
                {
                    createTime = value;
                }
            }

            private Instant updateTime;

            public Instant UpdateTime
            {
                get
                {
                    return updateTime;
                }

                set
                {
                    updateTime = value;
                }
            }
        }


        private static class Time
        {
            public static Instant GetCurrentTime()
            {
                return SystemClock.Instance.GetCurrentInstant();
            }
        }
    }
}
