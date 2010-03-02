using System.Text;
using Newtonsoft.Json.Linq;

namespace Raven.Database
{
    public class JsonDocument
    {
        public byte[] Data { get; set; }
        public JObject Metadata { get; set; }
        public string Key { get; set; }

        public JObject ToJson()
        {
            var doc = JObject.Parse(Encoding.UTF8.GetString(Data));
            doc.Add("@metadata", Metadata);
            return doc;
        }
    }
}