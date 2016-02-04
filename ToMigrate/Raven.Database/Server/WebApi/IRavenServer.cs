using Raven.Database.Config;

namespace Raven.Database.Server.WebApi
{
    public interface IRavenServer
    {
        DocumentDatabase SystemDatabase { get; }

        RavenConfiguration SystemConfiguration { get; }
    }
}
