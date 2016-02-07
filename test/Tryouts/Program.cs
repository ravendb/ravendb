using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Blittable.BlittableJsonWriterTests;
using FastTests.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Tests.Core;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                var objectJsonParsingTests = new BlittableFormatTests();
                objectJsonParsingTests.ShouldNotCrashForManyDifferentProperties().Wait();
            }
            //new PartialBlitable().CanSkipWritingPropertyNames().Wait();
        }
    }

    public class PartialBlitable
    {
        [Fact]
        public async Task CanSkipWritingPropertyNames()
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using(var ctx = new RavenOperationContext(pool))
            {
                var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes("{\"Name\":\"Oren\"}"));
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonStreamParser(memoryStream, ctx, state, "test"))
                {
                    var writer = new BlittableJsonDocumentBuilder(ctx, BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                        "test", parser, state);
                    ctx.CachedProperties.NewDocument();
                    var writeToken = await writer.ReadPartialObject();

                    writer.FinalizeDocumentWithoutProperties(writeToken, 1);
                    ctx.CachedProperties.Version = 1;
                    var reader = writer.CreateReader(ctx.CachedProperties);

                    Console.WriteLine(reader.Size);
                    string str;
                    Assert.True(reader.TryGet("Name", out str));
                    Assert.Equal("Oren", str);
                }
            }
        }
    }
}
