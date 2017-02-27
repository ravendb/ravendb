using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Raven.Server.Documents.Queries.Parse
{
    internal partial class LuceneQueryParser
    {
        public LuceneQueryParser() : base(null) { }

        public void Parse(string s)
        {
            byte[] inputBuffer = System.Text.Encoding.Default.GetBytes(s);
            MemoryStream stream = new MemoryStream(inputBuffer);
            this.Scanner = new LuceneQueryScanner(stream);
            this.Parse();
        }
    }
}
