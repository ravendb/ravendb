using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB
{
    public class Attachment
    {
        public byte[] Data { get; set; }
        public JObject Metadata { get; set; }
    }
}