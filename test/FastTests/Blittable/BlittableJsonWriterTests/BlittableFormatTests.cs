using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Sparrow.Json;
using Xunit;

namespace FastTests.Blittable.BlittableJsonWriterTests
{

    public class BlittableFormatTests : NoDisposalNeeded
    {
        [Theory]
        [MemberData("Samples")]
        public void CheckRoundtrip(string name)
        {
            using (var stream = typeof(BlittableFormatTests).GetTypeInfo().Assembly.GetManifestResourceStream(name))
            {
                var serializer = DocumentConvention.Default.CreateSerializer();

                var compacted = ((JObject)serializer.Deserialize(new JsonTextReader(new StreamReader(stream)))).ToString(Formatting.None);
                stream.Position = 0;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var writer = context.Read(stream, "docs/1");

                    var memoryStream = new MemoryStream();
                    context.Write(memoryStream, writer);
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
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var resource = typeof(BlittableFormatTests).Namespace + ".Jsons." + name;

                    using (var stream = typeof(BlittableFormatTests).GetTypeInfo().Assembly.GetManifestResourceStream(resource))
                    {
                        var serializer = DocumentConvention.Default.CreateSerializer();
                        
                        var compacted = ((JObject)serializer.Deserialize(new JsonTextReader(new StreamReader(stream)))).ToString(Formatting.None);
                        stream.Position = 0;

                        using (var writer = context.Read(stream, "docs/1 "))
                        {
                            var memoryStream = new MemoryStream();
                            context.Write(memoryStream, writer);
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