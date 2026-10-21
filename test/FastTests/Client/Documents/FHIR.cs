using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Newtonsoft.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client.Documents
{
    public class FHIR : RavenTestBase
    {
        public FHIR(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Fhir_Supports_Serialize_Deserialize()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = store =>
                {
                    var serializationConventions = (NewtonsoftJsonSerializationConventions)store.Conventions.Serialization;
                    serializationConventions.CustomizeJsonSerializer = serializer =>
                    {
                        serializer.Converters.Add(new FhirResourceConverter<Patient>());
                    };
                }
            }))
            {
                var id1 = Guid.NewGuid().ToString();
                var id2 = Guid.NewGuid().ToString();

                var pEvent1 = new Patient
                {
                    Id = id1,
                    Active = true
                };

                var pEvent2 = new Patient
                {
                    Id = id2,
                    Active = false
                };

                using (var session = store.OpenSession())
                {
                    var name1 = new HumanName
                    {
                        Family = "Patel",
                        Given = new List<string>
                        {
                            "Sandeep",
                            "M",
                            "Patel"
                        }
                    };

                    pEvent1.Name = new List<HumanName>(1) { name1 };
                    session.Store(pEvent1);

                    var name2 = new HumanName
                    {
                        Family = "Patel1",
                        Given = new List<string>
                        {
                            "Sandeep1",
                            "M1",
                            "Patel1"
                        }
                    };

                    pEvent2.Name = new List<HumanName>(1) { name2 };
                    session.Store(pEvent2);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    AssertPatient(id1, pEvent1);
                    AssertPatient(id2, pEvent2);

                    void AssertPatient(string id, Patient compareTo)
                    {
                        var patient = session.Load<Patient>(id);
                        var metadata = session.Advanced.GetMetadataFor(patient);
                        Assert.NotNull(metadata);

                        Assert.Equal(compareTo.Active, patient.Active);
                        Assert.Equal(compareTo.Name.Count, patient.Name.Count);

                        var compareToName = compareTo.Name[0];
                        var patientName = patient.Name[0];
                        Assert.Equal(compareToName.Family, patientName.Family);
                        Assert.True(compareToName.Given.SequenceEqual(patientName.Given));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Fhir_Supports_Serialize_Deserialize_As_Nested_Property()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = store =>
                {
                    var serializationConventions = (NewtonsoftJsonSerializationConventions)store.Conventions.Serialization;
                    serializationConventions.CustomizeJsonSerializer = serializer =>
                    {
                        serializer.Converters.Add(new FhirResourceConverter<Observation>());
                    };
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new ObservationHolder
                    {
                        Before = "before",
                        Observation = new Observation
                        {
                            Status = ObservationStatus.Final,
                            Value = new Quantity(1.50m, "mg")
                        },
                        After = 42
                    }, "holders/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var holder = session.Load<ObservationHolder>("holders/1");

                    Assert.Equal("before", holder.Before);
                    Assert.Equal(42, holder.After);
                    Assert.Equal(ObservationStatus.Final, holder.Observation.Status);

                    var quantity = Assert.IsType<Quantity>(holder.Observation.Value);
                    Assert.Equal("mg", quantity.Unit);
                    Assert.Equal(1.50m, quantity.Value);
                    Assert.Equal("1.50", quantity.Value.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }
            }
        }

        private class ObservationHolder
        {
            public string Before { get; set; }

            public Observation Observation { get; set; }

            public int After { get; set; }
        }

        public class FhirResourceConverter<T> : JsonConverter<T> where T : Base
        {
            private static readonly PocoBuilderSettings _builderSettings = new()
            {
                IgnoreUnknownMembers = true
            };

            public override T ReadJson(JsonReader reader, Type objectType, T existingValue, bool hasExistingValue, JsonSerializer serializer)
            {
                return FhirJsonNode.Read(reader).ToPoco<T>(_builderSettings);
            }

            public override void WriteJson(JsonWriter writer, T value, JsonSerializer serializer)
            {
                // Hl7.Fhir 6+ Serialize(Base, JsonWriter) passes the serialized resource to JsonWriter.WriteRaw (which leaves the writer
                // in an invalid state when the resource is not the root), so we write the serialized resource as JSON tokens instead
                var json = new FhirJsonSerializer().SerializeToString(value);

                using (var reader = new JsonTextReader(new StringReader(json)) { DateParseHandling = DateParseHandling.None, FloatParseHandling = FloatParseHandling.Decimal })
                {
                    writer.WriteToken(reader);
                }
            }
        }
    }
}
