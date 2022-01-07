
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Utils.Imports.Memory
{
    public partial class MemoryCache
    {
        public IEnumerable<KeyValuePair<object, object>> EntriesForDebug => _coherentState._entries.Select(kvp => new KeyValuePair<object, object>(kvp.Key, kvp.Value.Value));
    }
}
