// -----------------------------------------------------------------------
//  <copyright file="PartialBlittable.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Xunit;

namespace FastTests.Blittable
{

    public class PartialBlitable
    {
        [Fact]
        public async Task CanSkipWritingPropertyNames()
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var ctx = new RavenOperationContext(pool))
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