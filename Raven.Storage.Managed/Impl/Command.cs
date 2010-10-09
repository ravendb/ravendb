using Newtonsoft.Json.Linq;

namespace Raven.Storage.Managed.Impl
{
    public class Command
    {
        public int Size { get; set; }
        public JToken Key { get; set; }
        public CommandType Type { get; set; }
        public long Position { get; set; }
        public int DictionaryId { get; set; }
    }
}