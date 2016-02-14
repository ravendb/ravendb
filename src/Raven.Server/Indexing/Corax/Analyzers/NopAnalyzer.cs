using System.IO;

namespace Raven.Server.Indexing.Corax.Analyzers
{
    public class NopAnalyzer : IAnalyzer, ITokenSource
    {
        private TextReader _reader;

        public NopAnalyzer()
        {
            Buffer = new char[255];
        }

        public ITokenSource CreateTokenSource(string field, ITokenSource existing)
        {
            return this;
        }

        public bool Process(string field, ITokenSource source)
        {
            return true;
        }

        public int Size { get; set; }
        public int Line { get; private set; }
        public int Column { get; private set; }
        public int Position { get; set; }

        private bool _readFromReader;
        public bool Next()
        {
            if (_readFromReader)
                return false;
            _readFromReader = true;
            return true;
        }

        public void SetReader(TextReader reader)
        {
            _reader = reader;
            Size = _reader.ReadBlock(Buffer, 0, Buffer.Length);
            Line = 0;
            Column = 0;
            Position = 0;
            _readFromReader = false;
        }

        public char[] Buffer { get; }
    }
}