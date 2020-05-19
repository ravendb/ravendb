using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable.BlittableJsonWriterTests
{

    public class BlittableFormatTests : NoDisposalNeeded
    {
        public BlittableFormatTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [MemberData(nameof(Samples))]
        public void CheckRoundtrip(string name)
        {
            using (var stream = typeof(BlittableFormatTests).Assembly.GetManifestResourceStream(name))
            {
                var serializer = (JsonSerializer)DocumentConventions.Default.Serialization.CreateSerializer();

                var before = ((JObject)serializer.Deserialize(new JsonTextReader(new StreamReader(stream))));
                stream.Position = 0;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var writer = context.Read(stream, "docs/1");

                    var memoryStream = new MemoryStream();
                    context.Write(memoryStream, writer);

                    memoryStream.Position = 0;
                    var after = ((JObject)serializer.Deserialize(new JsonTextReader(new StreamReader(memoryStream))));

                    if (new JTokenEqualityComparer().Equals(before, after) == false)
                    {
                        Assert.Equal(before.ToString(Formatting.None), after.ToString(Formatting.None));
                    }
                }
            }
        }

        [Fact]
        public void InvalidJSon()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var invalid = @"{
 'User': 'ayende',
 'Age': 18,{'Error': 'ObjectDisposed'}";
                var stream = new MemoryStream(Encoding.UTF8.GetBytes(invalid));
                var invalidDataException = Assert.Throws<InvalidDataException>(() => context.Read(stream, "docs/1"));

                Assert.Contains(invalid, invalidDataException.Message);
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

                    using (var stream = typeof(BlittableFormatTests).Assembly.GetManifestResourceStream(resource))
                    {
                        var serializer = (JsonSerializer)DocumentConventions.Default.Serialization.CreateSerializer();

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
            var assembly = typeof(BlittableFormatTests).Assembly;

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
