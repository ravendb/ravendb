//-----------------------------------------------------------------------
// <copyright file="CreateFolderIcon.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;

namespace Raven.Database.Plugins.Builtins
{
	public class CreateFolderIcon : IStartupTask
	{
		private static ILog log = LogManager.GetCurrentClassLogger();

		public void Execute(DocumentDatabase database)
		{
			if (database.Configuration.RunInMemory)
				return;
			try
			{
				var dataDirectory = Path.GetFullPath(database.Configuration.DataDirectory);
				SetIconForFolder(dataDirectory);

				var tenantsPath = Directory.GetParent(dataDirectory);
				if (tenantsPath.Name == "Tenants")
					SetIconForFolder(dataDirectory);
			}
			catch (Exception e)
			{
				log.WarnException("Failed to create the appropriate Folder Icon for the RavenDB Data directory", e);
			}

		}

		private static void SetIconForFolder(string dataDirectory)
		{
			var desktopIni = Path.Combine(dataDirectory, "desktop.ini");
			var icon = Path.Combine(dataDirectory, "raven-data.ico");

			if (File.Exists(desktopIni) && File.Exists(icon))
				return;

			using (var iconFile = typeof (CreateFolderIcon).Assembly.GetManifestResourceStream("Raven.Database.Server.WebUI.raven-data.ico"))
			{
				File.WriteAllBytes(icon, iconFile.ReadData());
			}

			File.WriteAllText(desktopIni, string.Format(@"
[.ShellClassInfo]
IconResource={0},0
InfoTip=RavenDB's data folder. It is recommended that you will back up this folder on a regular basis.
[ViewState]
Mode=
Vid=
FolderType=Generic
",
			                                            icon));


			File.SetAttributes(desktopIni, FileAttributes.Hidden | FileAttributes.System | FileAttributes.Archive);
			File.SetAttributes(dataDirectory, FileAttributes.ReadOnly);
		}
	}
}
