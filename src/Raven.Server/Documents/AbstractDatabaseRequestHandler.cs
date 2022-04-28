using Raven.Server.ServerWide;
using Raven.Server.Web;

namespace Raven.Server.Documents;

public abstract class AbstractDatabaseRequestHandler : RequestHandler
{
    public abstract string DatabaseName { get; }

    public abstract OperationCancelToken CreateTimeLimitedOperationToken();
}
