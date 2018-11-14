
using System.Runtime.CompilerServices;

namespace Raven.Server.Indexing
{
    public class IndexOutputFilesSummary
    {
        public long TotalWritten { get; private set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Increment(long value)
        {
            TotalWritten += value;
        }

        public void Reset()
        {
            TotalWritten = 0;
        }
    }
}
