// -----------------------------------------------------------------------
//  <copyright file="UrlExtension.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Server.Runner.Imports
{
	public static class UrlExtension
	{
		public static string GetRequestUrl(this IHttpContext context)
		{
			var rawUrl = context.Request.RawUrl;
			return Raven.Database.Server.Abstractions.UrlExtension.GetRequestUrlFromRawUrl(rawUrl, context.Configuration);
		}
	}
}