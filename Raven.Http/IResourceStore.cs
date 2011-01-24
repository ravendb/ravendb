//-----------------------------------------------------------------------
// <copyright file="IResourceStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Http
{
    public interface IResourceStore : IDisposable
    {
        IRaveHttpnConfiguration Configuration { get; }
    }
}