// -----------------------------------------------------------------------
//  <copyright file="IISActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.DirectoryServices;
using System.Globalization;
using System.Linq;
using Microsoft.Deployment.WindowsInstaller;
using Microsoft.Web.Administration;
using Microsoft.Win32;
using Raven.Database.Util;

namespace Raven.Setup.CustomActions
{
	public class IISActions
	{
		private const string IISEntry = "IIS://localhost/W3SVC";
		private const string WebSiteProperty = "WEBSITE";
		private const string ServerComment = "ServerComment";
		private const string IISRegKey = @"Software\Microsoft\InetStp";
		private const string MajorVersion = "MajorVersion";
		private const string IISWebServer = "iiswebserver";
		private const string GetComboContent = "select * from ComboBox";
		private const string AvailableSites = "select * from AvailableWebSites";
		private const string SpecificSite = "select * from AvailableWebSites where WebSiteID=";
		private const string AppPoolProperty = "APPPOOL";
		private const string AvailableAppPools = "select * from AvailableApplicationPools";
		private const string SpecificAppPool = "select * from AvailableApplicationPools where PoolID=";
 
		[CustomAction]
        public static ActionResult GetWebSites(Session session)
		{
            try
            {
                var comboBoxView = session.Database.OpenView(GetComboContent);
                var availableSitesView = session.Database.OpenView(AvailableSites);

                if (IsIIS7Upwards)
                {
                    GetWebSitesViaWebAdministration(comboBoxView, availableSitesView);
                }
                else
                {
                    GetWebSitesViaMetabase(comboBoxView, availableSitesView);
                }

                return ActionResult.Success;
            }
            catch (Exception ex)
            {
				session.Log("Exception was thrown during GetWebSites custom action execution" + ex);
	            return ActionResult.Failure;
            }
        }

        [CustomAction]
        public static ActionResult UpdateIISPropsWithSelectedWebSite(Session session)
        {
            try
            {
                string selectedWebSiteId = session[WebSiteProperty];
                session.Log("CA: Found web site id: " + selectedWebSiteId);

                using (var availableWebSitesView = session.Database.OpenView(SpecificSite + selectedWebSiteId))
                {
                    availableWebSitesView.Execute();

                    using (Record record = availableWebSitesView.Fetch())
                    {
                        if ((record[1].ToString()) == selectedWebSiteId)
                        {
                            session["WEBSITE_ID"] = selectedWebSiteId;
                            session["WEBSITE_DESCRIPTION"] = (string)record[2];
                            session["WEBSITE_PATH"] = (string)record[3];
	                        session["WEBSITE_DEFAULT_APPPOOL"] = (string) record[4];

	                        //session.DoAction("SetIISInstallFolder");
                        }
                    }
                }

                return ActionResult.Success;
            }
            catch (Exception ex)
            {
				session.Log("Exception was thrown during UpdateIISPropsWithSelectedWebSite custom action execution" + ex.ToString());
	            return ActionResult.Failure;
            }
        }

        private static void GetWebSitesViaWebAdministration(View comboView, View availableView)
        {
            using (var iisManager = new ServerManager())
            {
                var order = 1;

                foreach (var webSite in iisManager.Sites)
                {
                    var id = webSite.Id.ToString(CultureInfo.InvariantCulture);
                    var name = webSite.Name;
                    var path = webSite.PhysicalPath();
	                var defaultAppPool = webSite.ApplicationDefaults.ApplicationPoolName;

                    StoreSiteDataInComboBoxTable(id, name, path, order++, comboView);
                    StoreSiteDataInAvailableSitesTable(id, name, path, defaultAppPool, availableView);
                }
            }
        }

        private static void GetWebSitesViaMetabase(View comboView, View availableView)
        {
            using (var iisRoot = new DirectoryEntry(IISEntry))
            {
                var order = 1;

                foreach (DirectoryEntry webSite in iisRoot.Children)
                {
                    if (webSite.SchemaClassName.ToLower(CultureInfo.InvariantCulture) == IISWebServer)
                    {
                        var id = webSite.Name;
                        var name = webSite.Properties[ServerComment].Value.ToString();
                        var path = webSite.PhysicalPath();
	                    var defaultAppPool = "";// TODO

                        StoreSiteDataInComboBoxTable(id, name, path, order++, comboView);
                        StoreSiteDataInAvailableSitesTable(id, name, path, defaultAppPool, availableView);
                    }
                }
            }
        }

        private static void StoreSiteDataInComboBoxTable(string id, string name, string physicalPath, int order, View comboView)
        {
            var newComboRecord = new Record(5);
            newComboRecord[1] = WebSiteProperty;
            newComboRecord[2] = order;
            newComboRecord[3] = id;
            newComboRecord[4] = name;
            newComboRecord[5] = physicalPath;
            comboView.Modify(ViewModifyMode.InsertTemporary, newComboRecord);
        }

        private static void StoreSiteDataInAvailableSitesTable(string id, string name, string physicalPath, string defaultAppPool, View availableView)
        {
            var newWebSiteRecord = new Record(4);
            newWebSiteRecord[1] = id;
            newWebSiteRecord[2] = name;
            newWebSiteRecord[3] = physicalPath;
			newWebSiteRecord[4] = defaultAppPool;
            availableView.Modify(ViewModifyMode.InsertTemporary, newWebSiteRecord);
        }

		[CustomAction]
		public static ActionResult GetAppPools(Session session)
		{
			session["IIS_APPPOOLS_INITIALIZED"] = "1";

			try
			{
				var comboBoxView = session.Database.OpenView(GetComboContent);
				var availableApplicationPoolsView = session.Database.OpenView(AvailableAppPools);

				IList<string> appPools;

				if (IsIIS7Upwards)
				{
					appPools = GetIis7UpwardsAppPools();
				}
				else
				{
					appPools = GetIis6AppPools();
				}

				for (var i = 0; i < appPools.Count; i++)
				{
					var newComboRecord = new Record(4);
					newComboRecord[1] = AppPoolProperty;
					newComboRecord[2] = i;
					newComboRecord[3] = i;
					newComboRecord[4] = appPools[i];

					comboBoxView.Modify(ViewModifyMode.InsertTemporary, newComboRecord);

					var newAppPoolRecord = new Record(2);
					newAppPoolRecord[1] = i;
					newAppPoolRecord[2] = appPools[i];
					availableApplicationPoolsView.Modify(ViewModifyMode.InsertTemporary, newAppPoolRecord);
				}

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				session.Log("Exception was thrown during GetAppPools custom action execution: " + ex);
				return ActionResult.Failure;
			}
		}

		[CustomAction]
		public static ActionResult UpdateIISPropsWithSelectedAppPool(Session session)
		{
			try
			{
				if (session["APPLICATION_POOL_TYPE"] == "NEW")
				{
					return ActionResult.Success;
				}

				var selectedAppPoolId = session[AppPoolProperty];

				using (var availableAppPoolsView = session.Database.OpenView(SpecificAppPool + selectedAppPoolId))
				{
					availableAppPoolsView.Execute();

					using (var record = availableAppPoolsView.Fetch())
					{
						if ((record[1].ToString()) == selectedAppPoolId)
						{
							session["WEB_APP_POOL_NAME"] = (string)record[2];
						}
					}
				}

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				session.Log("Exception was thrown during UpdateIISPropsWithSelectedWebSite custom action execution" + ex);
				return ActionResult.Failure;
			}
		}

		public static IList<string> GetIis7UpwardsAppPools()
		{
			var pools = new List<string>();

			using (var iisManager = new ServerManager())
			{
				pools.AddRange(iisManager.ApplicationPools.Where(p => p.ManagedPipelineMode == ManagedPipelineMode.Integrated && p.ManagedRuntimeVersion == "v4.0").Select(p => p.Name));
			}

			return pools;
		}

		private static IList<string> GetIis6AppPools()
		{
			var pools = new List<string>();
			using (var poolRoot = new DirectoryEntry("IIS://localhost/W3SVC/AppPools"))
			{
				poolRoot.RefreshCache();

				pools.AddRange(poolRoot.Children.Cast<DirectoryEntry>().Select(p => p.Name));
			}

			return pools;
		}

		[CustomAction]
		public static ActionResult UpdateIISAppPoolUser(Session session)
		{
			if (session["CUSTOM_APPLICATION_POOL"] != "1")
			{
				session["WEB_APP_POOL_IDENTITY_DOMAIN"] = "IIS AppPool";
				session["WEB_APP_POOL_IDENTITY_NAME"] = session["WEBSITE_DEFAULT_APPPOOL"];
				session["WEB_APP_POOL_NAME"] = session["WEBSITE_DEFAULT_APPPOOL"];
			}
			else
			{
				if (session["APPLICATION_POOL_TYPE"] == "NEW")
				{
					var identityType = session["APPLICATION_POOL_IDENTITY_TYPE"];

					if (identityType == "other")
					{
						return ActionResult.Success;
					}
					
					if (identityType == "ApplicationPoolIdentity")
					{
						session["WEB_APP_POOL_IDENTITY_DOMAIN"] = "IIS AppPool";
						session["WEB_APP_POOL_IDENTITY_NAME"] = session["WEB_APP_POOL_NAME"];
					}
					else if (identityType == "LocalService")
					{
						session["WEB_APP_POOL_IDENTITY_DOMAIN"] = "NT AUTHORITY";
						session["WEB_APP_POOL_IDENTITY_NAME"] = "LOCAL SERVICE";
					}
					else if (identityType == "LocalSystem")
					{
						session["WEB_APP_POOL_IDENTITY_DOMAIN"] = "NT AUTHORITY";
						session["WEB_APP_POOL_IDENTITY_NAME"] = "SYSTEM";
					}
					else if (identityType == "NetworkService")
					{
						session["WEB_APP_POOL_IDENTITY_DOMAIN"] = "NT AUTHORITY";
						session["WEB_APP_POOL_IDENTITY_NAME"] = "NETWORK SERVICE";
					}
				}
				else // existing app pool
				{
					session["WEB_APP_POOL_IDENTITY_DOMAIN"] = "IIS AppPool";
					session["WEB_APP_POOL_IDENTITY_NAME"] = session["WEB_APP_POOL_NAME"];
				}
			}

			return ActionResult.Success;
		}

		[CustomAction]
		public static ActionResult SetupPerformanceCountersForIISUser(Session session)
		{
			try
			{
				PerformanceCountersUtils.EnsurePerformanceCountersMonitoringAccess(string.Format("{0}\\{1}", session["WEB_APP_POOL_IDENTITY_DOMAIN"], session["WEB_APP_POOL_IDENTITY_NAME"]));
			}
			catch (Exception ex)
			{
				session.Log("Exception was thrown during SetupPerformanceCountersForIISUser custom action:" + ex);
				return ActionResult.Failure;
			}

			return ActionResult.Success;
		}

		private static bool IsIIS7Upwards
        {
            get
            {
                using (var iisKey = Registry.LocalMachine.OpenSubKey(IISRegKey))
                {
					if (iisKey == null)
						throw new Exception("IIS is not installed.");

	                return (int)iisKey.GetValue(MajorVersion) >= 7;
                }
            }
        }
	}
}