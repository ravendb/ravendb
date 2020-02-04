// -----------------------------------------------------------------------
//  <copyright file="IAsyncEnumerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
#if NETSTANDARD2_0 || NETCOREAPP2_1
    public interface IAsyncEnumerator<out T> : IDisposable
    {
        ValueTask<bool> MoveNextAsync();

        T Current { get; }

        ValueTask DisposeAsync();
    }
#endif
}
