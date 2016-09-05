using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Queries.Parse
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
            var luceneQueryScanner = new LuceneQueryScanner();
            luceneQueryScanner.SetSource(s, 0);
            Scanner = luceneQueryScanner;
            Parse();
        }
    }
}
