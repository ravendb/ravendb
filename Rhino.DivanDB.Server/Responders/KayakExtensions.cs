using System.IO;
using Kayak;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Server.Responders
{
    public static class KayakExtensions
    {
        public static JObject ReadJson(this KayakContext context)
        {
            using (var streamReader = new StreamReader(context.Request.InputStream))
            using (var jsonReader = new JsonTextReader(streamReader))
                return JObject.Load(jsonReader);
        }

        public static string ReadString(this KayakContext context)
        {
            using (var streamReader = new StreamReader(context.Request.InputStream))
                return streamReader.ReadToEnd();
        }

        public static void WriteJson(this KayakContext context, object obj)
        {
            new JsonSerializer().Serialize(context.Response.Output, obj);
        }
    }
}