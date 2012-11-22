using System;
using System.Collections.Generic;

namespace Raven.Database.Server.Security
{
	public class NeverSecret
	{
		public static readonly HashSet<string> Urls = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase)
			{
				// allow to get files that are static and are never secret, for example, the studio, the cross domain
				// policy and the fav icon
				"/",
				"/raven/studio.html",
				"/silverlight/Raven.Studio.xap",
				"/favicon.ico",
				"/databases",
				"/clientaccesspolicy.xml",
				"/build/version",
				"/OAuth/AccessToken",
				"/OAuth/API-Key",
			};
	}
}