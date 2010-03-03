using Newtonsoft.Json.Linq;

namespace Raven.Database
{
    public interface IDatabaseCommands
    {
        JsonDocument Get(string key);
        string Put(string key, JObject document, JObject metadata);
        void Delete(string key);
        string PutIndex(string name, string indexDef);
        QueryResult Query(string index, string query, int start, int pageSize);
        void DeleteIndex(string name);
        JArray GetDocuments(int start, int pageSize);
        JArray GetIndexNames(int start, int pageSize);
        JArray GetIndexes(int start, int pageSize);
    }
}