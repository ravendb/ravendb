using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace Raven.Server.Documents.Queries.Parse
{
    internal partial class LuceneQueryParser
    {
        public LuceneQueryParser() : base(null) { }
        private bool inMethod;
        public bool IsDefaultOperatorAnd { get; set; }
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
            var luceneQueryScanner = new LuceneQueryScanner();
            luceneQueryScanner.SetSource(s, 0);
            Scanner = luceneQueryScanner;
            Parse();
        }
    }
}
