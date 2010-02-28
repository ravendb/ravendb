using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Indexing;

namespace Rhino.DivanDB.Tasks
{
    public abstract class Task
    {
        public string View { get; set; }

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

        /// <summary>
        /// Tasks may NOT perform any writes operations on the TransactionalStorage!
        /// That is required because a failed task still commit  the TransactionalStorage 
        /// (to remove from the tasks).
        /// Another requirement is that executing task MUST be idempotent.
        /// </summary>
        public abstract void Execute(WorkContext context);
    }
}