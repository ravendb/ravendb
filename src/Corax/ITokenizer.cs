using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax.Tokenizers;

namespace Corax
{
    public interface ITokenizer<in TSource, out TOutput> : IDisposable
        where TSource : ITextSource
        where TOutput : struct, IEnumerator<TokenSpan>
    {
        TOutput Tokenize(TSource source);
    }
}
