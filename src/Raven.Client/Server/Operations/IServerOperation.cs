using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public interface IServerOperation<T>
    {
        RavenCommand<T> GetCommand(DocumentConventions conventions, JsonOperationContext context);
    }

    public interface IServerOperation
    {
        RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context);
    }
}