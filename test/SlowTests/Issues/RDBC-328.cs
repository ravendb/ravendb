using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBC_328:RavenTestBase
    {
        [Fact]
        public void CanCustomizeDeserialization()
        {
            using (var documentStore = GetDocumentStore(new Options
            {
                ModifyDocumentStore = x =>
                {
                    x.Conventions.CustomizeJsonDeserializer = s =>
                    {                     
                        s.Converters.Add(new NeverNullStringConverter());
                    };
                }
            }))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Entity { Id = "isnotnull", Property = new NeverNullString { Value = "hello" } });
                    session.Store(new Entity { Id = "isnull", Property = new NeverNullString() });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var isnotnull = session.Load<Entity>("isnotnull");
                    var isnull = session.Load<Entity>("isnull");
                    WaitForUserToContinueTheTest(documentStore);
                    Assert.NotNull(isnotnull.Property);
                    Assert.NotNull(isnull.Property);
                }
            }
        }

        [Fact]
        public void CanCustomizeSeserializationWithoutAffectingDeserialization()
        {
            using (var documentStore = GetDocumentStore(new Options
            {
                ModifyDocumentStore = x =>
                {
                    x.Conventions.CustomizeJsonSerializer = s =>
                    {
                        s.Converters.Add(new NeverNullStringConverter());
                    };
                }
            }))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Entity { Id = "isnotnull", Property = new NeverNullString { Value = "hello" } });
                    session.Store(new Entity { Id = "isnull", Property = new NeverNullString() });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    Assert.Equal(typeof(JsonSerializationException),
                    Assert.Throws<InvalidOperationException>(() => session.Load<Entity>("isnotnull")).InnerException.GetType());
                }
            }
        }

        public class Entity
        {
            public string Id { get; set; }
            public NeverNullString Property { get; set; }
        }
    }

    public class NeverNullString
    {
        private string _value;
        public string Value { get => _value ?? "<null>"; set => _value = value; }
    }

    public class NeverNullStringConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(NeverNullString);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return new NeverNullString { Value = reader.Value?.ToString() };
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var obj = (NeverNullString)value;
            var str = obj.Value == "<null>" ? null : obj.Value;
            serializer.Serialize(writer, str);
        }
    }
}

