// -----------------------------------------------------------------------
//  <copyright file="LoggingHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Microsoft.Deployment.WindowsInstaller;

namespace Raven.Setup.CustomActions
{
	public class Log
	{
		public static void Info(Session session, string message)
		{
			session.Log(message);
			session["RAVEN_INSTALLER_INFO"] = message;
		}

		public static void Error(Session session, string message)
		{
			session.Log(message);
			session["RAVEN_INSTALLER_ERROR"] = message;
		}
	}
}