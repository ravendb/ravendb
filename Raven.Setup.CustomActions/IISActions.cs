// -----------------------------------------------------------------------
//  <copyright file="IISActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.DirectoryServices;
using System.Globalization;
using Microsoft.Deployment.WindowsInstaller;
using Microsoft.Web.Administration;
using Microsoft.Win32;

namespace Raven.Setup.CustomActions
{
	public class IISActions
	{
		private const string IISEntry = "IIS://localhost/W3SVC";
		private const string SessionEntry = "WEBSITE";
		private const string ServerComment = "ServerComment";
		private const string IISRegKey = @"Software\Microsoft\InetStp";
		private const string MajorVersion = "MajorVersion";
		private const string IISWebServer = "iiswebserver";
		private const string GetComboContent = "select * from ComboBox";
		private const string AvailableSites = "select * from AvailableWebSites";
		private const string SpecificSite = "Select * from AvailableWebSites where WebSiteID=";
 
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
                string selectedWebSiteId = session[SessionEntry];
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

							session.DoAction("SetIISInstallFolder");
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

                    StoreSiteDataInComboBoxTable(id, name, path, order++, comboView);
                    StoreSiteDataInAvailableSitesTable(id, name, path, availableView);
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

                        StoreSiteDataInComboBoxTable(id, name, path, order++, comboView);
                        StoreSiteDataInAvailableSitesTable(id, name, path, availableView);
                    }
                }
            }
        }

        private static void StoreSiteDataInComboBoxTable(string id, string name, string physicalPath, int order, View comboView)
        {
            var newComboRecord = new Record(5);
            newComboRecord[1] = SessionEntry;
            newComboRecord[2] = order;
            newComboRecord[3] = id;
            newComboRecord[4] = name;
            newComboRecord[5] = physicalPath;
            comboView.Modify(ViewModifyMode.InsertTemporary, newComboRecord);
        }

        private static void StoreSiteDataInAvailableSitesTable(string id, string name, string physicalPath, View availableView)
        {
            var newWebSiteRecord = new Record(3);
            newWebSiteRecord[1] = id;
            newWebSiteRecord[2] = name;
            newWebSiteRecord[3] = physicalPath;
            availableView.Modify(ViewModifyMode.InsertTemporary, newWebSiteRecord);
        }
        private static bool IsIIS7Upwards
        {
            get
            {
                using (var iisKey = Registry.LocalMachine.OpenSubKey(IISRegKey))
                {
	                return (int)iisKey.GetValue(MajorVersion) >= 7;
                }
            }
        }
	}
}