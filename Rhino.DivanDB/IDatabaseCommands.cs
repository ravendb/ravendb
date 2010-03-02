using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB
{
    public interface IDatabaseCommands
    {
        byte[] Get(string key);
        string Put(JObject document);
        void Delete(string key);
        string PutIndex(string name, string indexDef);
        QueryResult Query(string index, string query, int start, int pageSize);
        void DeleteIndex(string name);
        JArray GetDocuments(int start, int pageSize);
        JArray GetIndexNames(int start, int pageSize);
        JArray GetIndexes(int start, int pageSize);
    }
}