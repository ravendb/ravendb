using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public interface ICollectionSessionOperations
    {
        /// <summary>
        /// Returns a collection fields
        /// </summary>
        BlittableJsonReaderObject GetCollectionFields(string collection, string prefix);

        /// <summary>
        /// Returns preview collection
        /// </summary>
        BlittableJsonReaderObject PreviewCollection(string collection);
    }

    public interface ICollectionSessionOperationsAsync
    {
        /// <summary>
        /// Returns a collection fields
        /// </summary>
        Task<BlittableJsonReaderObject> GetCollectionFieldsAsync(string collection, string prefix, CancellationToken token = default);

        /// <summary>
        /// Returns preview collection
        /// </summary>
        Task<BlittableJsonReaderObject> PreviewCollectionAsync(string collection, CancellationToken token = default);
    }
}
