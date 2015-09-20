using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Raven.Database.Config;
using Raven.Database.Server;

namespace Raven.Database.Common
{
	public interface IResourceApiController<TResource> : IResourceApiController
		where TResource : IResourceStore
	{
		new TResource Resource { get; }
	}

	public interface IResourceApiController
	{
		IResourceStore Resource { get; }
		InMemoryRavenConfiguration ResourceConfiguration { get; }
		string ResourceName { get; }
		ResourceType ResourceType { get; }
		InMemoryRavenConfiguration SystemConfiguration { get; }
		DocumentDatabase SystemDatabase { get; }
		Task<RequestWebApiEventArgs> TrySetupRequestToProperResource();
		List<Action<StringBuilder>> CustomRequestTraceInfo { get; }
		HttpRequestMessage InnerRequest { get; }
		int InnerRequestsCount { get; }
		bool IsInternalRequest { get; }
		void MarkRequestDuration(long elapsedMilliseconds);
		bool RejectClientRequests { get; }
	}
}