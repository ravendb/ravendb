// -----------------------------------------------------------------------
//  <copyright file="MigrationTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;

using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Tests.Common;
using Raven.Tests.Migration.Utils;

using Xunit;

namespace Raven.Tests.Migration
{
	public class MigrationTests : RavenTest
	{
		private const int Port = 8075;

		private const string DatabaseName = "Northwind";

		private const string PackageName = "RavenDB.Server";

		private readonly List<string> packageNames;

		public MigrationTests()
		{
			packageNames = ParseServers().ToList();
		}

		private static IEnumerable<string> ParseServers()
		{
			var document = XDocument.Load("../../packages.config");
			if (document.Root == null)
				yield break;

			var nodes = document.Root.Descendants().ToList();
			foreach (var node in nodes)
			{
				var attributes = node.Attributes().ToList();
				var id = attributes.First(x => x.Name == "id").Value;
				if (string.Equals(id, PackageName, StringComparison.OrdinalIgnoreCase) == false)
					continue;

				yield return PackageName + "." + attributes.First(x => x.Name == "version").Value;
			}
		}

		[Fact]
		public void T1()
		{
			foreach (var packageName in packageNames)
			{
				var backupLocation = NewDataPath(packageName + "-Backup", forceCreateDir: true);
				using (var client = new ThinClient(string.Format("http://localhost:{0}", Port), DatabaseName))
				{
					var generator = new DataGenerator(client, 10);

					using (DeployServer(packageName))
					{
						client.PutDatabase(DatabaseName);

						generator.Generate();

						client.StartBackup(DatabaseName, backupLocation, waitForBackupToComplete: true);
					}

					using (var store = NewRemoteDocumentStore(runInMemory: false))
					{
						store.DefaultDatabase = "Northwind";

						var operation = store
							.DatabaseCommands
							.GlobalAdmin
							.StartRestore(new DatabaseRestoreRequest
							{
								BackupLocation = backupLocation,
								DatabaseName = "Northwind"
							});

						operation.WaitForCompletion();

						WaitForUserToContinueTheTest(store);
					}
				}
			}
		}

		private ServerRunner DeployServer(string packageName)
		{
			var serverDirectory = NewDataPath(packageName, true);
			IOExtensions.CopyDirectory("../../../packages/" + packageName + "/tools/", serverDirectory);

			return ServerRunner.Run(Port, Path.Combine(serverDirectory, "Raven.Server.exe"));
		}
	}
}