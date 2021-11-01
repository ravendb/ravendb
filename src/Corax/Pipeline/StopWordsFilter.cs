using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Pipeline
{
    public struct StopWordsFilter : ITokenFilter
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Accept(ReadOnlySpan<byte> source, in Token token)
        {
            throw new NotImplementedException();
        }
    }
}
