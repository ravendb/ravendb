using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class MultiGetResult
    {
        public BlittableJsonReaderArray Results { get; set; }
    }
}