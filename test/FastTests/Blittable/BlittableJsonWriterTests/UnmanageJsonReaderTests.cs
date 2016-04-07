using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
            using (var ctx = new JsonOperationContext(pool))
            using (var stream = typeof(UnmanageJsonReaderTests).GetTypeInfo().Assembly.GetManifestResourceStream(name))
            using (var parser = new UnmanagedJsonParser(ctx, new JsonParserState(), "test"))
            {
                var buffer = new byte[4096];
                while (stream.Position != stream.Length)
                {
                    var read = stream.Read(buffer, 0, buffer.Length);
                    parser.SetBuffer(buffer, read);
                    while (parser.Read())
                    {
                        
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