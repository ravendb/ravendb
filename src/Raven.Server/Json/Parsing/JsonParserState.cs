using System;
using System.Collections.Generic;

namespace Raven.Server.Json.Parsing
{
    public unsafe class JsonParserState
    {
        public byte* StringBuffer;
        public int StringSize;
        public int? CompressedSize;
        public long Long;
        public JsonParserToken CurrentTokenType;
        public readonly List<int> EscapePositions = new List<int>();
    }
}