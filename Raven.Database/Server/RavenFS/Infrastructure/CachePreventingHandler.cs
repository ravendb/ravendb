using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Server.RavenFS.Infrastructure
{
	public class CachePreventingHandler : DelegatingHandler
	{
		protected override async Task<HttpResponseMessage> SendAsync(
			HttpRequestMessage request, CancellationToken cancellationToken)
		{
			var response = await base.SendAsync(request, cancellationToken);

			if (response.Headers != null)
			{
				if (response.Headers.CacheControl == null)
					response.Headers.CacheControl = new CacheControlHeaderValue();

				response.Headers.CacheControl.NoCache = true;
			}

			return response;
		}
	}
}