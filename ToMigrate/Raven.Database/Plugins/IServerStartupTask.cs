// -----------------------------------------------------------------------
//  <copyright file="IServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;

using Raven.Database.Server;

namespace Raven.Database.Plugins
{
    [InheritedExport]
    public interface IServerStartupTask : IDisposable
    {
        void Execute(RavenDBOptions serverOptions);
    }
}
