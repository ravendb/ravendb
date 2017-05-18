using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public interface IOperation<T>
    {
        RavenCommand<T> GetCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache);
    }

    public interface IOperation
    {
        RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache);
    }
}