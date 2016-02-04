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
    using Database.Server;

    public class PortActions
    {
        [CustomAction]
        public static ActionResult TryRevokePortReservation(Session session)
        {
            try
            {
                var portPropertyName = session["RAVEN_INSTALLATION_TYPE"].Equals("SERVICE") ? "SERVICE_PORT" : "WEBSITE_PORT";

                NonAdminHttp.TryUnregisterHttpPort(int.Parse(session[portPropertyName]), useSsl: false, hideWindow: true);
            }
            catch (Exception ex)
            {
                Log.Error(session, "Error occurred during TryRevokePortReservation. Exception: " + ex);
                return ActionResult.Success;
            }
            return ActionResult.Success;
        }

        [CustomAction]
        public static ActionResult CheckPortAvailability(Session session)
        {
            try
            {
                int port;

                var portPropertyName = session["RAVEN_INSTALLATION_TYPE"].Equals("SERVICE") ? "SERVICE_PORT" : "WEBSITE_PORT";

                if (int.TryParse(session[portPropertyName], out port))
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
