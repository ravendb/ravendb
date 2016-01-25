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
    public unsafe class UnmanageJsonReaderTests
    {
        [Theory]
        [MemberData("Samples")]
        public void CanReadAll(string name)
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var ctx = new RavenOperationContext(pool))
            using (var stream = typeof(UnmanageJsonReaderTests).GetTypeInfo().Assembly.GetManifestResourceStream(name))
            using (var state = new JsonParserState(ctx))
            using (var parser = new UnmanagedJsonParser(stream, ctx, state))
            {
                while (stream.Position != stream.Length)
                {
                    parser.Read();
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