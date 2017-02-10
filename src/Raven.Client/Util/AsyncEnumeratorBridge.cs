// -----------------------------------------------------------------------
//  <copyright file="AsyncEnumeratorBridge.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.NewClient.Abstractions.Util
{
    public class AsyncEnumeratorBridge<T> : IAsyncEnumerator<T>
    {
        private readonly IEnumerator<T> enumerator;

        public AsyncEnumeratorBridge(IEnumerator<T> enumerator)
        {
            this.enumerator = enumerator;
        }

        public void Dispose()
        {
            enumerator.Dispose();
        }

        public Task<bool> MoveNextAsync()
        {
            return new CompletedTask<bool>(enumerator.MoveNext());
        }

        public T Current { get { return enumerator.Current; } }
    }
}
