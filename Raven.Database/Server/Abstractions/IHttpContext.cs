using System.Security.Principal;

namespace Raven.Database.Server.Abstractions
{
	public interface IHttpContext
	{
		IHttpRequest Request { get; }
		IHttpResponse Response { get; }
		IPrincipal User { get; }
		void FinalizeResonse();
	}
}