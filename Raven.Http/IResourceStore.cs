//-----------------------------------------------------------------------
// <copyright file="IResourceStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Raven.Http
{
    public interface IResourceStore : IDisposable
    {
        IRaveHttpnConfiguration Configuration { get; }
		ConcurrentDictionary<string, object> ExternalState { get; set; }
    }
}