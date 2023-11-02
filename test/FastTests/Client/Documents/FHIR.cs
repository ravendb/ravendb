using System;
using System.Collections.Generic;
using System.Linq;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Newtonsoft.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

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
                var fhirSerializer = new FhirJsonSerializer();
                fhirSerializer.Serialize(value, writer);
            }
        }
    }
}
