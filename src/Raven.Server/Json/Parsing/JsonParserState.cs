using System;
using System.Collections.Generic;

namespace Raven.Server.Json.Parsing
{
    public class JsonParserState : IDisposable
    {
        public UnmanagedWriteBuffer StringBuffer;
        public long Long;
        public JsonParserToken Current;
        public readonly List<int> EscapePositions = new List<int>();

        public JsonParserState(RavenOperationContext ctx)
        {
            StringBuffer = new UnmanagedWriteBuffer(ctx);
        }

        public void Dispose()
        {
            StringBuffer?.Dispose();
        }
    }
}