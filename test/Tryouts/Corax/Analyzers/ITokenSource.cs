using System.IO;
using Raven.Server.Json;

namespace Tryouts.Corax.Analyzers
{
    public interface ITokenSource
    {
        int Size { get; set; }

        int Line { get; }
        int Column { get; }

        int Position { get; set; }

        bool Next();

        void SetReader(LazyStringValue reader);
        LazyStringValue GetCurrent();
    }
}