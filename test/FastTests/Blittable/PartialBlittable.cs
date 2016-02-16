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
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Xunit;

namespace FastTests.Blittable
{

    public class PartialBlitable
    {
        [Fact]
        public void CanSkipWritingPropertyNames()
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var ctx = new MemoryOperationContext(pool))
            {
                var buffer = Encoding.UTF8.GetBytes("{\"Name\":\"Oren\"}");
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
                {
                    parser.SetBuffer(buffer, buffer.Length);
                    var writer = new BlittableJsonDocumentBuilder(ctx, BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                        "test", parser, state);
                    ctx.CachedProperties.NewDocument();
                    writer.ReadObject();
                    Assert.True(writer.Read());

                    writer.FinalizeDocumentWithoutProperties(1);
                    ctx.CachedProperties.Version = 1;
                    var reader = writer.CreateReader(ctx.CachedProperties);

                    string str;
                    Assert.True(reader.TryGet("Name", out str));
                    Assert.Equal("Oren", str);
                }
            }
        }
    }
}