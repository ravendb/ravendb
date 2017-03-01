using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    public interface IMetadataDictionary : IDictionary<string, object>
    {
        bool TryGetValue(string key, out string value);
        string GetString(string key);
        IMetadataDictionary GetObject(string key);
        IMetadataDictionary[] GetObjects(string key);
    }
}