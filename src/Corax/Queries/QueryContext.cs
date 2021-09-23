using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    public static class QueryContext
    {
        public static readonly ArrayPool<byte> MatchesPool = ArrayPool<byte>.Create();
    }
}
