using System.Security.Principal;

namespace Raven.Server.Abstractions
{
	public interface IHttpContext
	{
		IHttpRequest Request { get; }
		IHttpResponse Response { get; }
		IPrincipal User { get; }
	}
}