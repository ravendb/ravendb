using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Pipeline
{
    public interface IFilter : IDisposable
    {
        int Filter(ReadOnlySpan<byte> source, ref Span<Token> tokens);
    }
}
