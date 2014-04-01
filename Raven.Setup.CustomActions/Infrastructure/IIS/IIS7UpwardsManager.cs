// -----------------------------------------------------------------------
//  <copyright file="IIS7Upward.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Web.Administration;

namespace Raven.Setup.CustomActions.Infrastructure.IIS
{
	public class IIS7UpwardsManager : IISManager
	{
		public IEnumerable<WebSite> GetWebSites()
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

		public IList<string> GetAppPools()
		{
			var pools = new List<string>();

			using (var iisManager = new ServerManager())
			{
				pools.AddRange(iisManager.ApplicationPools.Where(p => p.ManagedPipelineMode == ManagedPipelineMode.Integrated && p.ManagedRuntimeVersion == "v4.0").Select(p => p.Name));
			}

			return pools;
		}

		public void DisallowOverlappingRotation(string applicationPoolName)
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

		public void SetLoadUserProfile(string applicationPoolName)
		{
			using (var iisManager = new ServerManager())
			{
				foreach (var appPool in iisManager.ApplicationPools)
				{
					if (appPool.Name != applicationPoolName)
						continue;
					appPool.ProcessModel.LoadUserProfile = true;
					iisManager.CommitChanges();

					break;
				}
			}
		}
	}
}