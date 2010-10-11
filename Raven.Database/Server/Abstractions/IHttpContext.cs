using System;
using System.IO;
using System.Security.Principal;

namespace Raven.Database.Server.Abstractions
{
	public interface IHttpContext
	{
		RavenConfiguration Configuration { get; }
		IHttpRequest Request { get; }
		IHttpResponse Response { get; }
		IPrincipal User { get; }
		void FinalizeResonse();
		void SetResponseFilter(Func<Stream, Stream> responseFilter);
	}

	public static class UrlExtension
	{
		public static string GetRequestUrl(this IHttpContext context)
		{
			string localPath = context.Request.Url.LocalPath;
			if (context.Configuration.VirtualDirectory != "/" &&
				localPath.StartsWith(context.Configuration.VirtualDirectory, StringComparison.InvariantCultureIgnoreCase))
			{
				localPath = localPath.Substring(context.Configuration.VirtualDirectory.Length);
				if (localPath.Length == 0)
					localPath = "/";
			}
			return localPath;
		}
	}
}
