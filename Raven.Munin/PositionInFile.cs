using Newtonsoft.Json.Linq;

namespace Raven.Munin
{
    public class PositionInFile
    {
        public long Position { get; set; }
        public int Size { get; set; }
        public JToken Key { get; set; }
    }
}