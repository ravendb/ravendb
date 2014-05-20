// -----------------------------------------------------------------------
//  <copyright file="Context.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

using Raven.Database.Extensions;
using Raven.Server;

namespace Raven.Tests.Server.Runner.Data
{
	public static class Context
	{
		public static readonly string DataDir = string.Format(@"Servers");

		public static readonly IDictionary<int, RavenDbServer> Servers = new Dictionary<int, RavenDbServer>();

		public static void Clear()
		{
			foreach (var serverPort in Servers.Keys)
				Servers[serverPort].Dispose();

			GC.Collect(2);
			GC.WaitForPendingFinalizers();

			DeleteDataDirectoryWithRetry();
		}

		private static void DeleteDataDirectoryWithRetry()
		{
			var isRetry = false;

			while (true)
			{
				try
				{
					DeleteDirectory(DataDir);
					break;
				}
				catch (Exception)
				{
					if (isRetry)
						throw;

					GC.Collect();
					GC.WaitForPendingFinalizers();
					isRetry = true;
				}
			}
		}

		public static void DeleteDirectory(string directory)
		{
			IOExtensions.DeleteDirectory(directory);
			SpinWait.SpinUntil(() => Directory.Exists(directory) == false, TimeSpan.FromSeconds(5));
		}
	}
}