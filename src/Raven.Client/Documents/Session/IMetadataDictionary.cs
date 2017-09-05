using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    public interface IMetadataDictionary : IDictionary<string, object>
    {
        bool TryGetValue(string key, out string value);
        string GetString(string key);
        long GetLong(string key);
        bool GetBoolean(string key);
        double GetDouble(string key);
        IMetadataDictionary GetObject(string key);
        IMetadataDictionary[] GetObjects(string key);
    }
}
