using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Server.Json;
using Xunit;
using Formatting = Raven.Imports.Newtonsoft.Json.Formatting;

namespace BlittableTests.BlittableJsonWriterTests
{
    public unsafe class BlittableFormatTests
    {
        [Theory]
        [MemberData("Samples")]
        public void CheckRoundtrip(string name)
        {
            string origin;
            using (var stream = typeof(BlittableFormatTests).GetTypeInfo().Assembly.GetManifestResourceStream(name))
            {
                origin = new StreamReader(stream).ReadToEnd();
            }
            var compacted = JObject.Parse(origin).ToString(Formatting.None);
            using (var pool = new UnmanagedBuffersPool("test", 1024 * 1024))
            using (var context = new RavenOperationContext(pool))
            {
                var writer = context.Read(new JsonTextReader(new StringReader(origin))
                {
                    DateParseHandling = DateParseHandling.None
                }, "docs/1");

                int size;
                var ptr = pool.GetMemory(writer.SizeInBytes, out size);
                writer.CopyTo(ptr);
                var reader = new BlittableJsonReaderObject(ptr, writer.SizeInBytes, context);

                var memoryStream = new MemoryStream();
                reader.WriteTo(memoryStream, originalPropertyOrder: true);
                var s = Encoding.UTF8.GetString(memoryStream.ToArray());

                JObject.Parse(s); // can parse the output

                Assert.Equal(compacted, s);
            }
        }

        [Theory]
        public void ShouldNotCrashForManyDifferentProperties()
        {
            foreach (var name in new[] {"geo.json", "comments.json", "blog_post.json"})
            {
                using (var pool = new UnmanagedBuffersPool("test", 1024 * 1024))
                using (var context = new RavenOperationContext(pool))
                {
                    string origin;
                    var resource = typeof(BlittableFormatTests).Namespace + ".Jsons." + name;
                    Console.WriteLine(resource);
                    using (var stream = typeof(BlittableFormatTests).GetTypeInfo().Assembly
                        .GetManifestResourceStream(resource))
                    {
                        origin = new StreamReader(stream).ReadToEnd();
                    }
                    var compacted = JObject.Parse(origin).ToString(Formatting.None);

                    var writer = context.Read(new JsonTextReader(new StringReader(origin))
                    {
                        DateParseHandling = DateParseHandling.None
                    }, "docs/1 ");

                    int size;
                    var ptr = pool.GetMemory(writer.SizeInBytes, out size);
                    writer.CopyTo(ptr);
                    var reader = new BlittableJsonReaderObject(ptr, writer.SizeInBytes, context);

                    var memoryStream = new MemoryStream();
                    reader.WriteTo(memoryStream, originalPropertyOrder: true);
                    var s = Encoding.UTF8.GetString(memoryStream.ToArray());

                    JObject.Parse(s); // can parse the output

                    Assert.Equal(compacted, s);
                }
            }
        }

        public static IEnumerable<object[]> Samples()
        {
            var assembly = typeof(BlittableFormatTests).GetTypeInfo().Assembly;

            foreach (var name in assembly.GetManifestResourceNames())
            {
                if (Path.GetExtension(name) == ".json")
                {
                    yield return new object[] { name };
                }
            }
        }
    }
}