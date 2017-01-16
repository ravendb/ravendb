using Raven.NewClient.Client.Commands;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document
{
    public delegate bool AfterStreamExecutedDelegate(BlittableJsonReaderObject document);
}
