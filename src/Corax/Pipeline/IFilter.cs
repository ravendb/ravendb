using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Pipeline
{
    public interface IFilter : IDisposable
    {
        bool SupportUtf8 => false;


        int Filter(ReadOnlySpan<byte> source, ref Span<Token> tokens);
        
        int Filter(ReadOnlySpan<char> source, ref Span<Token> tokens);
    }
}
