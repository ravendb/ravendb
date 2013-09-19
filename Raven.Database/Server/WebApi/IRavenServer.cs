using Raven.Database.Config;

namespace Raven.Database.Server.WebApi
{
	public interface IRavenServer
	{
		DocumentDatabase SystemDatabase { get; }

		InMemoryRavenConfiguration SystemConfiguration { get; }
	}
}
