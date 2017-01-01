using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Queries.Parse
{
    internal partial class LuceneQueryParser
    {
        public LuceneQueryParser() : base(null) { }

        private bool _inMethod;
        public bool InMethod
        {
            get
            {
                return _inMethod;
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                ((LuceneQueryScanner)Scanner).InMethod = value;
                _inMethod = value;
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
