using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax
{
    public interface ITokenFilter<in TSource, out TOutput> : IDisposable
        where TSource : IEnumerator<TokenSpan>
        where TOutput : struct, IEnumerator<TokenSpan>
    {
        TOutput Filter(TSource source);
    }
}
