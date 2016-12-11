// -----------------------------------------------------------------------
//  <copyright file="IResourceLandlord.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Database.Config;

namespace Raven.Database.Common
{
    public interface IResourceLandlord<TResource> : IDisposable
        where TResource : IResourceStore
    {
        Task<TResource> GetResourceInternal(string resourceName);

        bool TryGetOrCreateResourceStore(string resourceName, out Task<TResource> resourceTask);

        ConcurrentDictionary<string, DateTime> LastRecentlyUsed { get; }

        InMemoryRavenConfiguration GetSystemConfiguration();
    }
}
