using System.IO;

namespace Raven.Server.Indexing.Corax.Analyzers
{
    public interface ITokenSource
    {
        int Size { get; set; }

        int Line { get; }
        int Column { get; }

        int Position { get; set; }

        bool Next();

        void SetReader(TextReader reader);
        
        char[] Buffer { get; }
    }
}