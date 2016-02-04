using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

using Raven.Imports.Newtonsoft.Json.Converters;
using Raven.Server.Json;
using Xunit;

namespace NewBlittable.Tests.BlittableJsonWriterTests
{
    public class VariousPropertyAmountsTests
    {
        public ExpandoObject GenerateExpandoObject(int depth = 1, int width = 8, bool reuseFieldNames = true)
        {
            if (depth <= 0 || width <= 0)
                throw new ArgumentException("Illegal depth or width");

            if (depth == 1)
            {
                var curExpando = new ExpandoObject();
                var cuExpandoAsDictionary = (IDictionary<string, object>)curExpando;
                for (var i = 0; i < width; i++)
                {
                    cuExpandoAsDictionary["Field" + i] = i.ToString();
                }

                return curExpando;
            }



            var expando = new ExpandoObject();
            var expandoAsDictionary = (IDictionary<string, object>)expando;
            for (var i = 0; i < width; i++)
            {
                expandoAsDictionary["Field" + i] = GenerateExpandoObject(depth - 1, width, reuseFieldNames);
            }

            return expando;
        }

        public string GetJsonString(int depth = 1, int width = 8, bool reuseFieldNames = true)
        {
            var expando = GenerateExpandoObject(depth, width, reuseFieldNames);

            JsonSerializerSettings serializerSettings = new JsonSerializerSettings();
            serializerSettings.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter());
            serializerSettings.Converters.Add(new Newtonsoft.Json.Converters.ExpandoObjectConverter());

            JsonSerializer serializer = JsonSerializer.Create(serializerSettings);

            var stringWriter = new StringWriter();
            var jsonWriter = new JsonTextWriter(stringWriter);
            serializer.Serialize(jsonWriter, expando);
            return stringWriter.ToString();
        }

        public class CustomMemberBinder : GetMemberBinder
        {
            public CustomMemberBinder(string name, bool ignoreCase) : base(name, ignoreCase)
            {
            }

            public override DynamicMetaObject FallbackGetMember(DynamicMetaObject target, DynamicMetaObject errorSuggestion)
            {
                throw new NotImplementedException();
            }
        }


        [Theory]
        [InlineData(byte.MaxValue)]
        [InlineData(short.MaxValue)]
        [InlineData(short.MaxValue + 1)]
        public async Task FlatBoundarySizeFieldsAmount(int maxValue)
        {
            //var maxValue = short.MaxValue + 1000;
            var str = GetJsonString(1, maxValue);

            var unmanagedPool = new UnmanagedBuffersPool(string.Empty);

            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = await blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {

                System.Dynamic.DynamicObject dynamicBlittableJObject = new DynamicBlittableJson(employee);

                for (var i = 0; i < maxValue; i++)
                {
                    object curVal;
                    Assert.True(dynamicBlittableJObject.TryGetMember(new CustomMemberBinder("Field" + i, true), out curVal));
                    Assert.Equal(curVal.ToString(), i.ToString());
                }
            }
        }

        [Theory]
        [InlineData(byte.MaxValue)]
        [InlineData(short.MaxValue)]
        [InlineData(short.MaxValue + 1)]
        public async Task FlatBoundarySizeFieldsAmountStreamRead(int maxValue)
        {

            var str = GetJsonString(1, maxValue);

            var unmanagedPool = new UnmanagedBuffersPool(string.Empty);

            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = await blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                var ms = new MemoryStream();
                employee.WriteTo(ms, originalPropertyOrder: true);

                Assert.Equal(Encoding.UTF8.GetString(ms.ToArray()), str);
            }
        }
    }
}
