using Sparrow.Json;

namespace Raven.Client.Commands
{
    public class BlittableArrayResult
    {
        public BlittableJsonReaderArray Results { get; set; }
    }
}