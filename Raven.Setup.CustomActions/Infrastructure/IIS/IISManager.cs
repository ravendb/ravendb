// -----------------------------------------------------------------------
//  <copyright file="IISManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Setup.CustomActions.Infrastructure.IIS
{
	public interface IISManager
	{
		IEnumerable<WebSite> GetWebSites();

		IList<string> GetAppPools();

		void DisallowOverlappingRotation(string applicationPoolName);
		void SetLoadUserProfile(string applicationPoolName);
	}
}