// -----------------------------------------------------------------------
//  <copyright file="IISActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using Microsoft.Deployment.WindowsInstaller;
using Microsoft.Win32;
using Raven.Database.Util;
using Raven.Setup.CustomActions.Infrastructure.IIS;

namespace Raven.Setup.CustomActions
{
	public class IISActions
	{
		private const string WebSiteProperty = "WEBSITE";

		private const string IISRegKey = @"Software\Microsoft\InetStp";
		private const string MajorVersion = "MajorVersion";
		private const string GetComboContent = "select * from ComboBox";
		private const string AvailableSites = "select * from AvailableWebSites";
		private const string SpecificSite = "select * from AvailableWebSites where WebSiteID=";
		private const string AppPoolProperty = "APPPOOL";
		private const string AvailableAppPools = "select * from AvailableApplicationPools";
		private const string SpecificAppPool = "select * from AvailableApplicationPools where PoolID=";
		private const string AsteriskSiteId = "*";

		private static readonly IISManager iisManager;

		static IISActions()
		{
			if (IsIIS7Upwards)
			{
				iisManager = new IIS7UpwardsManager();
			}
			else
			{
				iisManager = new IIS6Manager();
			}
		}


		[CustomAction]
        public static ActionResult GetWebSites(Session session)
		{
			session["IIS_WEBSITES_INITIALIZED"] = "1";

            try
            {
	            var webSites = iisManager.GetWebSites();

	            var order = 1;
	            foreach (var webSite in webSites)
	            {
		            StoreSiteDataInComboBoxTable(session, webSite, order++);
		            StoreSiteDataInAvailableSitesTable(session, webSite);
	            }

                return ActionResult.Success;
            }
            catch (Exception ex)
            {
				Log.Error(session, "Exception was thrown during GetWebSites execution: " + ex);
	            return ActionResult.Failure;
            }
        }

        [CustomAction]
        public static ActionResult UpdateIISPropsWithSelectedWebSite(Session session)
        {
            try
            {
				if (session["WEBSITE_TYPE"] == "EXISTING")
				{
					string selectedWebSiteId = session[WebSiteProperty];
					Log.Info(session, "Found web site id: " + selectedWebSiteId);

					using (var availableWebSitesView = session.Database.OpenView(SpecificSite + selectedWebSiteId))
					{
						availableWebSitesView.Execute();

						using (var record = availableWebSitesView.Fetch())
						{
							if ((record[1].ToString()) == selectedWebSiteId)
							{
								session["WEBSITE_ID"] = selectedWebSiteId;
								session["WEBSITE_DESCRIPTION"] = (string)record[2];
								session["WEBSITE_PATH"] = (string)record[3];
								session["WEBSITE_DEFAULT_APPPOOL"] = (string)record[4];
							}
						}
					}
				}
				else
				{
					session["WEBSITE_ID"] = AsteriskSiteId;
					session["WEBSITE_DEFAULT_APPPOOL"] = "DefaultAppPool";
				}
				session.DoAction("SetIISInstallFolder");

                return ActionResult.Success;
            }
            catch (Exception ex)
            {
				Log.Error(session, "Exception was thrown during UpdateIISPropsWithSelectedWebSite execution" + ex);
	            return ActionResult.Failure;
            }
        }

		private static void StoreSiteDataInComboBoxTable(Session session, WebSite webSite, int order)
        {
			var comboBoxTable = session.Database.OpenView(GetComboContent);

            var newComboRecord = new Record(5);
            newComboRecord[1] = WebSiteProperty;
            newComboRecord[2] = order;
			newComboRecord[3] = webSite.Id;
			newComboRecord[4] = webSite.Name;
			newComboRecord[5] = webSite.PhysicalPath;

			comboBoxTable.Modify(ViewModifyMode.InsertTemporary, newComboRecord);
        }

        private static void StoreSiteDataInAvailableSitesTable(Session session, WebSite webSite)
        {
			var availableSitesTable = session.Database.OpenView(AvailableSites);

            var newWebSiteRecord = new Record(4);
            newWebSiteRecord[1] = webSite.Id;
            newWebSiteRecord[2] = webSite.Name;
            newWebSiteRecord[3] = webSite.PhysicalPath;
			newWebSiteRecord[4] = webSite.DefaultAppPool;

			availableSitesTable.Modify(ViewModifyMode.InsertTemporary, newWebSiteRecord);
        }

		[CustomAction]
		public static ActionResult GetAppPools(Session session)
		{
			session["IIS_APPPOOLS_INITIALIZED"] = "1";

			try
			{
				var comboBoxView = session.Database.OpenView(GetComboContent);
				var availableApplicationPoolsView = session.Database.OpenView(AvailableAppPools);

				var appPools = iisManager.GetAppPools();

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
				Log.Error(session, "Exception was thrown during GetAppPools execution: " + ex);
				return ActionResult.Failure;
			}
		}

		[CustomAction]
		public static ActionResult SetApplicationPoolIdentityType(Session session)
		{
			try
			{
				session["APPLICATION_POOL_IDENTITY_TYPE_INITIALIZED"] = "1";

				var comboBoxView = session.Database.OpenView(GetComboContent);

				var availableAppPoolIdentities = new Dictionary<string, string>();

				if (IsIIS7Upwards)
				{
					availableAppPoolIdentities.Add("ApplicationPoolIdentity", "ApplicationPoolIdentity");

					session["APPLICATION_POOL_IDENTITY_TYPE"] = "ApplicationPoolIdentity";
				}
				else
				{
					session["APPLICATION_POOL_IDENTITY_TYPE"] = "other";
				}

				availableAppPoolIdentities.Add("LocalService", "LocalService");
				availableAppPoolIdentities.Add("LocalSystem", "LocalSystem");
				availableAppPoolIdentities.Add("NetworkService", "NetworkService");
				availableAppPoolIdentities.Add("Other", "other");

				var order = 1;

				foreach (var identityType in availableAppPoolIdentities)
				{
					var newComboRecord = new Record(4);
					newComboRecord[1] = "APPLICATION_POOL_IDENTITY_TYPE";
					newComboRecord[2] = order;
					newComboRecord[3] = identityType.Value;
					newComboRecord[4] = identityType.Key;

					comboBoxView.Modify(ViewModifyMode.InsertTemporary, newComboRecord);

					order++;
				}
			}
			catch (Exception ex)
			{
				Log.Error(session, "Failed to SetApplicationPoolIdentityType. Exception: " + ex.Message);
				return ActionResult.Failure;
			}

			return ActionResult.Success;
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
				Log.Error(session, "Exception was thrown during UpdateIISPropsWithSelectedWebSite: " + ex);
				return ActionResult.Failure;
			}
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
				Log.Error(session, "Exception was thrown during SetupPerformanceCountersForIISUser:" + ex);

				var sb =
					new StringBuilder(
						string.Format("Warning: The access to performance counters has not been configured for the account '{0}\\{1}'.{2}",
						              session["WEB_APP_POOL_IDENTITY_DOMAIN"], session["WEB_APP_POOL_IDENTITY_NAME"], Environment.NewLine));


				if (ex is IdentityNotMappedException)
				{
					sb.Append("The account does not exist.");
				}
				else
				{
					sb.Append("Exception type: " + ex.GetType());
				}

				if (string.IsNullOrEmpty(session["LOG_FILE_PATH"]) == false)
				{
					sb.Append(string.Format("{0}For more details check the log file:{0}{1}", Environment.NewLine, session["LOG_FILE_PATH"]));
				}

				MessageBox.Show(sb.ToString(), "Failed to grant permissions", MessageBoxButtons.OK, MessageBoxIcon.Warning);

				return ActionResult.Success;
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

		[CustomAction]
		public static ActionResult OpenWebSiteDirectoryChooser(Session session)
		{
			try
			{
				var task = new Thread(() =>
				{
					var fileDialog = new FolderBrowserDialog { ShowNewFolderButton = true };
					if (fileDialog.ShowDialog() == DialogResult.OK)
					{
						session["WEBSITE_PATH"] = fileDialog.SelectedPath;
					}

					session.DoAction("SetNewWebSiteDirectory");
				});
				task.SetApartmentState(ApartmentState.STA);
				task.Start();
				task.Join();

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during OpenWebSiteDirectoryChooser. Exception: " + ex);
				return ActionResult.Failure;
			}
			
		}

		[CustomAction]
		public static ActionResult FindIdOfCreatedWebSite(Session session)
		{
			try
			{
				if (session["WEBSITE_ID"] != AsteriskSiteId) // id was set by selecting existing web site
					return ActionResult.Success;

				var site = iisManager.GetWebSites().First(x => x.Name == session["WEBSITE_DESCRIPTION"]);

				session["WEBSITE_ID"] = site.Id;

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during FindIdOfCreatedWebSite. Exception: " + ex);
				return ActionResult.Failure;
			}
		}

		[CustomAction]
		public static ActionResult DisallowApplicationPoolOverlappingRotation(Session session)
		{
			try
			{
				iisManager.DisallowOverlappingRotation(session["WEB_APP_POOL_NAME"]);

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during DisallowOverlappingRotation. Exception: " + ex);
				return ActionResult.Failure;
			}
			
		}

		[CustomAction]
		public static ActionResult SetLoadUserProfile(Session session)
		{
			try
			{
				iisManager.SetLoadUserProfile(session["WEB_APP_POOL_NAME"]);

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during DisallowOverlappingRotation. Exception: " + ex);
				return ActionResult.Failure;
			}

		}
	}
}
