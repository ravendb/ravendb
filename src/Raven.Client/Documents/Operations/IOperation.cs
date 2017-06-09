using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public interface IOperation<T>
    {
        RavenCommand<T> GetCommand(IDocumentStore store, JsonOperationContext context, HttpCache cache);
    }

    public interface IOperation
    {
        RavenCommand GetCommand(IDocumentStore store, JsonOperationContext context, HttpCache cache);
    }
}