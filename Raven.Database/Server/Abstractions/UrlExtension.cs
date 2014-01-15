//-----------------------------------------------------------------------
// <copyright file="UrlExtension.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Web.Http;
using Raven.Database.Config;
using Raven.Database.Server.Controllers;

namespace Raven.Database.Server.Abstractions
{
	public static class UrlExtension
	{
		public static string GetRequestUrl(this IHttpContext context)
		{
			var rawUrl = context.Request.RawUrl;
			return GetRequestUrlFromRawUrl(rawUrl, context.Configuration);
		}

		public static string GetRequestUrl(this RavenDbApiController controller)
		{
			var rawUrl = controller.InnerRequest.RequestUri.AbsoluteUri;
			return GetRequestUrlFromRawUrl(rawUrl, controller.DatabasesLandlord.SystemConfiguration);
		}

		public static string GetRequestUrlFromRawUrl(string rawUrl, InMemoryRavenConfiguration configuration)
		{
			string localPath = rawUrl;
			var indexOfQuery = localPath.IndexOf('?');
			if (indexOfQuery != -1)
				localPath = localPath.Substring(0, indexOfQuery);
			if (localPath.StartsWith("//"))
				localPath = localPath.Substring(1);
			if (configuration.VirtualDirectory != "/" &&
			    localPath.StartsWith(configuration.VirtualDirectory, StringComparison.OrdinalIgnoreCase))
			{
				localPath = localPath.Substring(configuration.VirtualDirectory.Length);
				if (localPath.Length == 0)
					localPath = "/";
			}
			return localPath;
		}
	}
}