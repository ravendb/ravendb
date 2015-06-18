using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace Raven.Database.Indexing
{
    internal partial class LuceneQueryParser
    {
        public LuceneQueryParser() : base(null) { }
        private bool inMethod;
        public bool InMethod
        {
            get
            {
                return inMethod;
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                ((LuceneQueryScanner)Scanner).InMethod = value;
                inMethod = value;
            }
        }
        public void Parse(string s)
        {
            byte[] inputBuffer = System.Text.Encoding.Default.GetBytes(s);
            MemoryStream stream = new MemoryStream(inputBuffer);
            this.Scanner = new LuceneQueryScanner(stream);
            this.Parse();
        }
    }
}
