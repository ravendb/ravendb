using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public delegate bool AfterStreamExecutedDelegate(BlittableJsonReaderObject document);
}
