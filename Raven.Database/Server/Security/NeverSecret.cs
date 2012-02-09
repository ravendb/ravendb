namespace Raven.Database.Server.Security
{
	public class NeverSecret
	{
		public static readonly string[] Urls = new[]
			{
				// allow to get files that are static and are never secret, for example, the studio, the cross domain
				// policy and the fav icon
				"/",
				"/raven/studio.html",
				"/silverlight/Raven.Studio.xap",
				"/favicon.ico",
				"/clientaccesspolicy.xml",
				"/build/version",
				"/OAuth/AccessToken",
			};
	}
}