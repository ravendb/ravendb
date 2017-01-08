using System.IO;
using System.Runtime.CompilerServices;

namespace Raven.Database.Indexing
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
