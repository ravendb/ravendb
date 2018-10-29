using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12194 : RavenTestBase
    {
        [Fact]
        public void Can_load_with_JsonConverter()
        {
            using (var documentStore1 = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = x => x.Conventions = new DocumentConventions
                {
                    CustomizeJsonSerializer = s =>
                    {
                        s.Converters.Add(new FrankenJsonConverter());
                    }
                }
            }))
            using (var documentStore2 = GetDocumentStore(new Options()))
            {
                using (var session = documentStore1.OpenSession())
                {
                    session.Store(new TestDocument
                    {
                        Id = "test/1",
                        Betrag = new Franken(30)
                    });
                    session.SaveChanges();
                }

                using (var session = documentStore2.OpenSession())
                {
                    session.Store(new OtherDocument
                    {
                        Id = "other/1",
                        Name = "Other1"
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(documentStore1);
                WaitForIndexing(documentStore2);

                Parallel.For(1, 15, index =>
                {
                    using (var session2 = documentStore2.OpenSession())
                    {
                        var other = session2.Load<OtherDocument>("other/1");

                        Assert.NotNull(other);

                        using (var session1 = documentStore1.OpenSession())
                        {
                            var test1 = session1.Load<TestDocument>("test/1");

                            Assert.NotNull(test1);

                            var tests = session1.Query<TestDocument>().ToList();

                            Assert.Equal(1, tests.Count);
                        }

                        var others = session2.Query<OtherDocument>().ToList();
                        Assert.Equal(1, others.Count);
                    }
                });
            }
        }
    }

    public class TestDocument
    {
        public string Id { get; set; }
        public Franken Betrag { get; set; }
    }

    public class OtherDocument
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class Franken
    {
        public decimal Value { get; }

        public Franken(decimal value)
        {
            this.Value = value;
        }
    }

    public class FrankenJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var franken = (Franken)value;

            writer.WriteValue(franken.Value);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var number = Decimal.Parse(reader.Value.ToString());

            return new Franken(number);
        }

        public override bool CanConvert(Type objectType) => typeof(Franken) == objectType;
    }
}

