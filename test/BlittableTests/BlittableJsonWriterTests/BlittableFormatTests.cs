using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Bond;
using Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Server.Json;
using Xunit;
using Formatting = Raven.Imports.Newtonsoft.Json.Formatting;
using Marshal = System.Runtime.InteropServices.Marshal;

namespace BlittableTests.BlittableJsonWriterTests
{
    public unsafe class BlittableFormatTests
    {
        [Theory]
        [MemberData("Samples")]
        public void CheckRoundtrip(string name)
        {
            using (var stream = typeof(BlittableFormatTests).GetTypeInfo().Assembly.GetManifestResourceStream(name))
            {
                var compacted = JObject.Parse(new StreamReader(stream).ReadToEnd()).ToString(Formatting.None);
                stream.Position = 0;
                using (var pool = new UnmanagedBuffersPool("test") )
                using (var context = new RavenOperationContext(pool))
                {
                    var writer = context.Read(stream, "docs/1");

                    var memoryStream = new MemoryStream();
                    writer.WriteTo(memoryStream, originalPropertyOrder: true);
                    var s = Encoding.UTF8.GetString(memoryStream.ToArray());

                    JObject.Parse(s); // can parse the output

                    Assert.Equal(compacted, s);
                }
            }
        }

        [Fact]
        public void ShouldNotCrashForManyDifferentProperties()
        {
            foreach (var name in new[] { "geo.json", "comments.json", "blog_post.json" })
            {
                using (var pool = new UnmanagedBuffersPool("test"))
                using (var context = new RavenOperationContext(pool))
                {
                    string origin;
                    var resource = typeof(BlittableFormatTests).Namespace + ".Jsons." + name;
                    
                    using (var stream = typeof(BlittableFormatTests).GetTypeInfo().Assembly
                        .GetManifestResourceStream(resource))
                    {
                        origin = new StreamReader(stream).ReadToEnd();
                        stream.Position = 0;
                        var compacted = JObject.Parse(origin).ToString(Formatting.None);

                        using (var writer = context.Read(stream, "docs/1 "))
                        {

                            var memoryStream = new MemoryStream();
                            writer.WriteTo(memoryStream, originalPropertyOrder: true);
                            var s = Encoding.UTF8.GetString(memoryStream.ToArray());

                            JObject.Parse(s); // can parse the output

                            Assert.Equal(compacted, s);
                        }
                    }
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