// -----------------------------------------------------------------------
//  <copyright file="ErrorActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Deployment.WindowsInstaller;

namespace Raven.Setup.CustomActions
{
	public class LoggingActions
	{
		[CustomAction]
		public static ActionResult SetLogFileLocation(Session session)
		{
			var locationFromMsi = session["MsiLogFileLocation"]; // this variable is available from MSI 4.0
			if (string.IsNullOrEmpty(locationFromMsi) == false)
			{
				session["LOG_FILE_PATH"] = locationFromMsi;
			}
			else
			{
				var ravenInstallerLogs =
					Directory.GetFiles(Path.GetTempPath(), @"RavenDB.Setup?????.log")
					         .Where(x => Regex.IsMatch(Path.GetFileName(x), @"RavenDB.Setup\d{5,5}\.log"));
				var logFile = ravenInstallerLogs.LastOrDefault(); // take the newest one

				if (logFile != null)
				{
					session["LOG_FILE_PATH"] = logFile;
				}
			}

			return ActionResult.Success;
		}

	}
}