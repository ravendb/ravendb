using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries.Graph
{
    public static class GraphQueryExtension
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CheckIfCancellationIsRequested(this OperationCancelToken token)
        {
            if (token.IsCancellationRequested)
                throw new OperationCanceledException();
        }
    }
}
