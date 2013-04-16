// -----------------------------------------------------------------------
//  <copyright file="LoggingHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Microsoft.Deployment.WindowsInstaller;

namespace Raven.Setup.CustomActions
{
	public class LoggingHelper
	{
		public static void Log(Session session, string message)
		{
			session.Log(message);
			session["INSTALLER_LOG"] = message;
		}
	}
}