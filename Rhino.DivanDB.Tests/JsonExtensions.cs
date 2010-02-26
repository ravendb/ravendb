using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Tests
{
    public static class JsonExtensions
    {
        public static JObject ToJson(this string self)
        {
            return JObject.Parse(self);
        }
    }
}