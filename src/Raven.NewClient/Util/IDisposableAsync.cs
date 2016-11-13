// -----------------------------------------------------------------------
//  <copyright file="IDisposableAsync.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Util
{
    public interface IDisposableAsync
    {
        Task DisposeAsync();
    }
}
