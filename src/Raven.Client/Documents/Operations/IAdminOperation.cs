using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public interface IAdminOperation<T>
    {
        RavenCommand<T> GetCommand(DocumentConventions conventions, JsonOperationContext context);
    }

    public interface IAdminOperation
    {
        RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context);
    }
}