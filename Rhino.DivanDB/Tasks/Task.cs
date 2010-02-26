using System.IO;
using Newtonsoft.Json;

namespace Rhino.DivanDB.Tasks
{
    public abstract class Task
    {
        public string Type
        {
            get
            {
                return GetType().Name;
            }
        }

        public string AsString()
        {
            var stringWriter = new StringWriter();
            new JsonSerializer().Serialize(stringWriter, this);
            return stringWriter.GetStringBuilder().ToString();
        }
    }
}