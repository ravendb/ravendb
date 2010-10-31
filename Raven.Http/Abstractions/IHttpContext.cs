using System;
using System.IO;
using System.Security.Principal;

namespace Raven.Http.Abstractions
{
	public interface IHttpContext
	{
        IRaveHttpnConfiguration Configuration { get; }
		IHttpRequest Request { get; }
		IHttpResponse Response { get; }
		IPrincipal User { get; }
		void FinalizeResonse();
		void SetResponseFilter(Func<Stream, Stream> responseFilter);
	}
}
