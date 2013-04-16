// -----------------------------------------------------------------------
//  <copyright file="ServiceActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Net.NetworkInformation;
using Microsoft.Deployment.WindowsInstaller;

namespace Raven.Setup.CustomActions
{
	public class ServiceActions
	{
		[CustomAction]
		public static ActionResult CheckPortAvailability(Session session)
		{
			try
			{
				int port;

				if (int.TryParse(session["SERVICE_PORT"], out port))
				{
					var activeTcpListeners = IPGlobalProperties
					.GetIPGlobalProperties()
					.GetActiveTcpListeners();

					if (activeTcpListeners.All(endPoint => endPoint.Port != port))
					{
						session["PORT_AVAILABILITY"] = "Port available";
					}
					else
					{
						session["PORT_AVAILABILITY"] = "Port in use";
					}
				}
				else
				{
					session["PORT_AVAILABILITY"] = "Invalid port format";
				}

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during CheckPortAvailability. Exception: " + ex);
				return ActionResult.Failure;
			}
		} 
	}
}
