// -----------------------------------------------------------------------
//  <copyright file="IIS7Upward.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Web.Administration;

namespace Raven.Setup.CustomActions.Infrastructure.IIS
{
	public static class IIS7UpwardsManager
	{
		public static IEnumerable<WebSite> GetWebSitesViaWebAdministration()
		{
			using (var iisManager = new ServerManager())
			{
				foreach (var webSite in iisManager.Sites)
				{
					var webSiteModel = new WebSite();

					webSiteModel.Id = webSite.Id.ToString(CultureInfo.InvariantCulture);
					webSiteModel.Name = webSite.Name;
					webSiteModel.PhysicalPath = webSite.PhysicalPath();
					webSiteModel.DefaultAppPool = webSite.ApplicationDefaults.ApplicationPoolName;

					yield return webSiteModel;
				}
			}
		}

		public static void DisallowOverlappingRotation(string applicationPoolName)
		{
			using (var iisManager = new ServerManager())
			{
				foreach (var appPool in iisManager.ApplicationPools)
				{
					if (appPool.Name != applicationPoolName) 
						continue;
					appPool.Recycling.DisallowOverlappingRotation = true;
					iisManager.CommitChanges();

					break;
				}
			}
		}
	}
}