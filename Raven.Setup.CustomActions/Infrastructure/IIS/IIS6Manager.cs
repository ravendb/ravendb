// -----------------------------------------------------------------------
//  <copyright file="IIS6.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.DirectoryServices;
using System.Globalization;
using System.Linq;

namespace Raven.Setup.CustomActions.Infrastructure.IIS
{
	public class IIS6Manager : IISManager
	{
		private const string IISEntry = "IIS://localhost/W3SVC";
		private const string IISWebServer = "iiswebserver";
		private const string ServerComment = "ServerComment";
		private const string ApplicationPoolsEntry = "IIS://localhost/W3SVC/AppPools";

		public IEnumerable<WebSite> GetWebSites()
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

		public IList<string> GetAppPools()
		{
			var pools = new List<string>();
			using (var poolRoot = new DirectoryEntry(ApplicationPoolsEntry))
			{
				poolRoot.RefreshCache();

				pools.AddRange(poolRoot.Children.Cast<DirectoryEntry>().Select(p => p.Name));
			}

			return pools;
		}

		public void DisallowOverlappingRotation(string applicationPoolName)
		{
			using (var applicationPool = new DirectoryEntry(string.Format("{0}/{1}", ApplicationPoolsEntry, applicationPoolName)))
			{
				applicationPool.Properties["DisallowOverlappingRotation"].Value = true;
				applicationPool.CommitChanges();
			}
		}

		public void SetLoadUserProfile(string applicationPoolName)
		{
			// not applicable for IIS 6
		}
	}
}