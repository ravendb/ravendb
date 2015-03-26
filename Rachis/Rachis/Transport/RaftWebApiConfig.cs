// -----------------------------------------------------------------------
//  <copyright file="w.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Web.Http;

namespace Rachis.Transport
{
	public static class RaftWebApiConfig
	{
		public static void Load()
		{
			// calling this force .NET to load this assembly, so then you can call MapHttpAttributeRoutes
		}
	}
}