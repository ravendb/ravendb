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
            // TODO: These are placeholder implementations in order to ensure that pipeline functionality is sound arquitecturally
            //       proper implementations are needed for production and running full test suit using the Corax engine. 

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Accept(ReadOnlySpan<char> source, in Token token)
        {
            // TODO: These are placeholder implementations in order to ensure that pipeline functionality is sound arquitecturally
            //       proper implementations are needed for production and running full test suit using the Corax engine. 

            return true;
        }
    }
}
