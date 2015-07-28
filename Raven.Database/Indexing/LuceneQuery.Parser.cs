using System.IO;
using System.Runtime.CompilerServices;

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
            var stream = new MemoryStream(inputBuffer);
            Scanner = new LuceneQueryScanner(stream);
            Parse();
        }
    }
}
