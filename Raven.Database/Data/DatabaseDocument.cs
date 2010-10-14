using System.Collections.Generic;

namespace Raven.Database.Data
{
    public class DatabaseDocument
    {
        public string Id { get; set; }
        public string DataDirectory { get; set; }
        public Dictionary<string, string> Settings { get; set; }
    }
}