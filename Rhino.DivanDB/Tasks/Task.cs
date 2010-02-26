using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Indexing;

namespace Rhino.DivanDB.Tasks
{
    public abstract class Task
    {
        public string Type
        {
            get
            {
                return GetType().FullName;
            }
        }

        public string AsString()
        {
            var stringWriter = new StringWriter();
            new JsonSerializer().Serialize(stringWriter, this);
            return stringWriter.GetStringBuilder().ToString();
        }

        public static Task ToTask(string task)
        {
            var json = JObject.Parse(task);
            var typename = json.Property("Type").Value.Value<string>();
            var type = typeof (Task).Assembly.GetType(typename);
            return (Task)new JsonSerializer().Deserialize(new StringReader(task), type);
        }

        public abstract void Execute(WorkContext context);
    }
}