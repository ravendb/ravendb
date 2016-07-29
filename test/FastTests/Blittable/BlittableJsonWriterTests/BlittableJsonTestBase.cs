using System.IO;
using System.Text;
using Raven.Abstractions.Linq;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public abstract class BlittableJsonTestBase 
    {
        public string GenerateSimpleEntityForFunctionalityTest()
        {
            object employee = new
            {
                Name = "Oren",
                Age = "34",
                Dogs = new[] { "Arava", "Oscar", "Phoebe" },
                Office = new
                {
                    Name = "Hibernating Rhinos",
                    Street = "Hanais 21",
                    City = "Hadera"
                }
            };

            return JsonConvert.SerializeObject(employee);
        }

        public string GenerateSimpleEntityForFunctionalityTest2()
        {
            return @"{""<Name>i__Field"":""Oren"",""<Age>i__Field"":""34"",""<Dogs>i__Field"":[""Arava"",""Oscar"",""Phoebe""],""<MegaDevices>i__Field"":[{""<Name>i__Field"":""Misteryous Brain Disruptor"",""<Usages>i__Field"":0,""Name"":""Misteryous Brain Disruptor"",""Usages"":0},{""<Name>i__Field"":""Hockey stick"",""<Usages>i__Field"":4,""Name"":""Hockey stick"",""Usages"":4}],""<Office>i__Field"":{""<Manager>i__Field"":{""<Name>i__Field"":""Assi"",""<Id>i__Field"":44,""Name"":""Assi"",""Id"":44},""<Name>i__Field"":""Hibernating Rhinos"",""<Street>i__Field"":""Hanais 21"",""<City>i__Field"":""Hadera"",""Manager"":{""<Name>i__Field"":""Assi"",""<Id>i__Field"":44,""Name"":""Assi"",""Id"":44},""Name"":""Hibernating Rhinos"",""Street"":""Hanais 21"",""City"":""Hadera""},""Name"":""Oren"",""Age"":""34"",""Dogs"":[""Arava"",""Oscar"",""Phoebe""],""MegaDevices"":[{""<Name>i__Field"":""Misteryous Brain Disruptor"",""<Usages>i__Field"":0,""Name"":""Misteryous Brain Disruptor"",""Usages"":0},{""<Name>i__Field"":""Hockey stick"",""<Usages>i__Field"":4,""Name"":""Hockey stick"",""Usages"":4}],""Office"":{""<Manager>i__Field"":{""<Name>i__Field"":""Assi"",""<Id>i__Field"":44,""Name"":""Assi"",""Id"":44},""<Name>i__Field"":""Hibernating Rhinos"",""<Street>i__Field"":""Hanais 21"",""<City>i__Field"":""Hadera"",""Manager"":{""<Name>i__Field"":""Assi"",""<Id>i__Field"":44,""Name"":""Assi"",""Id"":44},""Name"":""Hibernating Rhinos"",""Street"":""Hanais 21"",""City"":""Hadera""}}";
//            return @"{
//  ""<Name>i__Field"": ""Oren"",
//  ""<Age>i__Field"": ""34"",
//  ""<Dogs>i__Field"": [
//    ""Arava"",
//    ""Oscar"",
//    ""Phoebe""
//  ],
//  ""<MegaDevices>i__Field"": [
//    {
//      ""<Name>i__Field"": ""Misteryous Brain Disruptor"",
//      ""<Usages>i__Field"": 0,
//      ""Name"": ""Misteryous Brain Disruptor"",
//      ""Usages"": 0
//    },
//    {
//      ""<Name>i__Field"": ""Hockey stick"",
//      ""<Usages>i__Field"": 4,
//      ""Name"": ""Hockey stick"",
//      ""Usages"": 4
//    }
//  ],
//  ""<Office>i__Field"": {
//    ""<Manager>i__Field"": {
//      ""<Name>i__Field"": ""Assi"",
//      ""<Id>i__Field"": 44,
//      ""Name"": ""Assi"",
//      ""Id"": 44
//    },
//    ""<Name>i__Field"": ""Hibernating Rhinos"",
//    ""<Street>i__Field"": ""Hanais 21"",
//    ""<City>i__Field"": ""Hadera"",
//    ""Manager"": {
//      ""<Name>i__Field"": ""Assi"",
//      ""<Id>i__Field"": 44,
//      ""Name"": ""Assi"",
//      ""Id"": 44
//    },
//    ""Name"": ""Hibernating Rhinos"",
//    ""Street"": ""Hanais 21"",
//    ""City"": ""Hadera""
//  },
//  ""Name"": ""Oren"",
//  ""Age"": ""34"",
//  ""Dogs"": [
//    ""Arava"",
//    ""Oscar"",
//    ""Phoebe""
//  ],
//  ""MegaDevices"": [
//    {
//      ""<Name>i__Field"": ""Misteryous Brain Disruptor"",
//      ""<Usages>i__Field"": 0,
//      ""Name"": ""Misteryous Brain Disruptor"",
//      ""Usages"": 0
//    },
//    {
//      ""<Name>i__Field"": ""Hockey stick"",
//      ""<Usages>i__Field"": 4,
//      ""Name"": ""Hockey stick"",
//      ""Usages"": 4
//    }
//  ],
//  ""Office"": {
//    ""<Manager>i__Field"": {
//      ""<Name>i__Field"": ""Assi"",
//      ""<Id>i__Field"": 44,
//      ""Name"": ""Assi"",
//      ""Id"": 44
//    },
//    ""<Name>i__Field"": ""Hibernating Rhinos"",
//    ""<Street>i__Field"": ""Hanais 21"",
//    ""<City>i__Field"": ""Hadera"",
//    ""Manager"": {
//      ""<Name>i__Field"": ""Assi"",
//      ""<Id>i__Field"": 44,
//      ""Name"": ""Assi"",
//      ""Id"": 44
//    },
//    ""Name"": ""Hibernating Rhinos"",
//    ""Street"": ""Hanais 21"",
//    ""City"": ""Hadera""
//  }
//}";
        }

        protected static unsafe void AssertComplexEmployee(string str,BlittableJsonReaderObject doc,
         JsonOperationContext blittableContext)
        {
            dynamic dynamicRavenJObject = new DynamicJsonObject(RavenJObject.Parse(str));
            dynamic dynamicBlittableJObject = new DynamicBlittableJson(doc);

            Assert.Equal(dynamicRavenJObject.Age, dynamicBlittableJObject.Age);
            Assert.Equal(dynamicRavenJObject.Name, dynamicBlittableJObject.Name);
            Assert.Equal(dynamicRavenJObject.Dogs.Count, dynamicBlittableJObject.Dogs.Count);
            for (var i = 0; i < dynamicBlittableJObject.Dogs.Length; i++)
            {
                Assert.Equal(dynamicRavenJObject.Dogs[i], dynamicBlittableJObject.Dogs[i]);
            }
            Assert.Equal(dynamicRavenJObject.Office.Name, dynamicRavenJObject.Office.Name);
            Assert.Equal(dynamicRavenJObject.Office.Street, dynamicRavenJObject.Office.Street);
            Assert.Equal(dynamicRavenJObject.Office.City, dynamicRavenJObject.Office.City);
            Assert.Equal(dynamicRavenJObject.Office.Manager.Name, dynamicRavenJObject.Office.Manager.Name);
            Assert.Equal(dynamicRavenJObject.Office.Manager.Id, dynamicRavenJObject.Office.Manager.Id);

            Assert.Equal(dynamicRavenJObject.MegaDevices.Count, dynamicBlittableJObject.MegaDevices.Count);
            for (var i = 0; i < dynamicBlittableJObject.MegaDevices.Length; i++)
            {
                Assert.Equal(dynamicRavenJObject.MegaDevices[i].Name,
                    dynamicBlittableJObject.MegaDevices[i].Name);
                Assert.Equal(dynamicRavenJObject.MegaDevices[i].Usages,
                    dynamicBlittableJObject.MegaDevices[i].Usages);
            }
            var ms = new MemoryStream();
            blittableContext.Write(ms, doc);
            
            Assert.Equal(str, Encoding.UTF8.GetString(ms.ToArray()));
        }
    }
}
