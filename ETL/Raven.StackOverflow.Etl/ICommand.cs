using System.Collections.Generic;
using System.IO;

namespace Raven.StackOverflow.Etl
{
    public interface ICommand
    {
        string CommandText { get; }
        void Run();

        void LoadArgs(IEnumerable<string> remainingArgs);
        void WriteHelp(TextWriter tw);
    }
}