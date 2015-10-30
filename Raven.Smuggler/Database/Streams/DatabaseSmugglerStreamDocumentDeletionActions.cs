using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Streams
{
    public class DatabaseSmugglerStreamDocumentDeletionActions : DatabaseSmugglerStreamActionsBase, IDatabaseSmugglerDocumentDeletionActions
    {
        public DatabaseSmugglerStreamDocumentDeletionActions(JsonTextWriter writer)
            : base(writer, "DocsDeletions")
        {
        }

        public Task WriteDocumentDeletionAsync(string key, CancellationToken cancellationToken)
        {
            var o = new RavenJObject
                    {
                        {"Key", key}
                    };
            o.WriteTo(Writer);
            return new CompletedTask();
        }
    }
}
