// -----------------------------------------------------------------------
//  <copyright file="IServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;

using Raven.Server;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public interface IServerStartupTask
	{
		void Execute(RavenDbServer server);
	}
}