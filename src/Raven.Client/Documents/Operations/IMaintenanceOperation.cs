using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public interface IMaintenanceOperation<T>
    {
        RavenCommand<T> GetCommand(DocumentConventions conventions, JsonOperationContext context);
    }

    public interface IMaintenanceOperation
    {
        RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context);
    }
}
