using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Xunit;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public unsafe class UnmanageJsonReaderTests
    {
        [Theory]
        [MemberData("Samples")]
        public void CanReadAll(string name)
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var ctx = new RavenOperationContext(pool))
            using (var stream = typeof(UnmanageJsonReaderTests).GetTypeInfo().Assembly.GetManifestResourceStream(name))
            using (var parser = new UnmanagedJsonStreamParser(stream, ctx, new JsonParserState(), "test"))
            {
                while (stream.Position != stream.Length)
                {
                    parser.ReadAsync().Wait();
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