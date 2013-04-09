// -----------------------------------------------------------------------
//  <copyright file="IIS6.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.DirectoryServices;
using System.Globalization;

namespace Raven.Setup.CustomActions.Infrastructure.IIS
{
	public static class IIS6Manager
	{
		private const string IISEntry = "IIS://localhost/W3SVC";
		private const string IISWebServer = "iiswebserver";
		private const string ServerComment = "ServerComment";

		public static IEnumerable<WebSite> GetWebSitesViaMetabase()
		{
			using (var iisRoot = new DirectoryEntry(IISEntry))
			{
				foreach (DirectoryEntry webSiteEntry in iisRoot.Children)
				{
					if (webSiteEntry.SchemaClassName.ToLower(CultureInfo.InvariantCulture) == IISWebServer)
					{
						var webSiteModel = new WebSite();

						webSiteModel.Id = webSiteEntry.Name;
						webSiteModel.Name = webSiteEntry.Properties[ServerComment].Value.ToString();
						webSiteModel.PhysicalPath = webSiteEntry.PhysicalPath();
						webSiteModel.DefaultAppPool = (string)webSiteEntry.Properties["AppPoolId"].Value;

						yield return webSiteModel;
					}
				}
			}
		}
	}
}