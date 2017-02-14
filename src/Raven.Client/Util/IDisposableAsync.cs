// -----------------------------------------------------------------------
//  <copyright file="IDisposableAsync.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;

namespace Raven.Client.Util
{
    internal interface IDisposableAsync
    {
        Task DisposeAsync();
    }
}
