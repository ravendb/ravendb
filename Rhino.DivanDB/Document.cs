using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB
{
    public class JsonDocument
    {
        public byte[] Data { get; set; }
        public JObject Metadata { get; set; }

        public JObject ToJson()
        {
            var doc = JObject.Parse(Encoding.UTF8.GetString(Data));
            doc.Add("@metadata", Metadata);
            return doc;
        }
    }
}