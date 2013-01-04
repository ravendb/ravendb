//-----------------------------------------------------------------------
// <copyright file="IStartupTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Database.Server;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public interface IStartupTask
	{
		void Execute(DocumentDatabase database);
	}

	[InheritedExport]
	public interface IServerStartupTask
	{
		void Execute(HttpServer server);
	}
}
