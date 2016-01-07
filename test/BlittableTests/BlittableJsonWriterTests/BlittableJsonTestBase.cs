using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConsoleApplication4;
using Raven.Abstractions.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.Json;
using Xunit;

namespace NewBlittable.Tests.BlittableJsonWriterTests
{
    public class BlittableJsonTestBase
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

            var jsonSerializer = new JsonSerializer();
            var stringWriter = new StringWriter();
            var jsonWriter = new JsonTextWriter(stringWriter);
            jsonSerializer.Serialize(jsonWriter, employee);

            return stringWriter.ToString();
        }

        public string GenerateSimpleEntityForFunctionalityTest2()
        {
            object employee = new
            {
                Name = "Oren",
                Age = "34",
                Dogs = new[] { "Arava", "Oscar", "Phoebe" },
                MegaDevices = new[]
                {
                  new
                  {
                      Name = "Misteryous Brain Disruptor",
                      Usages = 0
                  }  ,
                  new
                  {
                      Name="Hockey stick",
                      Usages = 4
                  }
                },
                Office = new
                {
                    Manager = new
                    {
                        Name = "Assi",
                        Id = 44
                    },
                    Name = "Hibernating Rhinos",
                    Street = "Hanais 21",
                    City = "Hadera"
                }
            };

            var jsonSerializer = new JsonSerializer();
            var stringWriter = new StringWriter();
            var jsonWriter = new JsonTextWriter(stringWriter);
            jsonSerializer.Serialize(jsonWriter, employee);

            return stringWriter.ToString();
        }

        protected static unsafe void AssertComplexEmployee(string str, byte* ptr, BlittableJsonWriter employee,
         RavenOperationContext blittableContext)
        {
            dynamic dynamicRavenJObject = new DynamicJsonObject(RavenJObject.Parse(str));
            dynamic dynamicBlittableJObject = new DynamicBlittableJson(ptr, employee.SizeInBytes,
                blittableContext);

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
        }
    }
}
