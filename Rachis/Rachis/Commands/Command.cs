using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Rachis.Commands
{
    public abstract class Command
    {
        public long AssignedIndex { get; set; }

        public byte[] ToBytes()
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(this, new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.Objects }));
        }

        public static T FromBytes<T>(byte[] bytes)
        {
            return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(bytes, 0, bytes.Length),new JsonSerializerSettings() {TypeNameHandling = TypeNameHandling.Objects });
        }

    }
}
