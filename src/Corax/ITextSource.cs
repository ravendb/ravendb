using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax
{
    public interface ITextSource : IDisposable
    {
        Span<byte> Peek(int size);
        void Consume(int size = -1);
        TokenSpan Retrieve(int length, int type = TokenType.None);

        void Reset();
    }
}
