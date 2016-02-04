using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Smuggler.Common;

namespace Raven.Smuggler.Database.Streams
{
    public class DatabaseSmugglerStreamIdentityActions : SmugglerStreamActionsBase, IDatabaseSmugglerIdentityActions
    {
        public DatabaseSmugglerStreamIdentityActions(JsonTextWriter writer)
            : base(writer, "Identities")
        {
        }

        public Task WriteIdentityAsync(string name, long value, CancellationToken cancellationToken)
        {
            new RavenJObject
            {
                { "Key", name },
                { "Value", value }
            }.WriteTo(Writer);

            return new CompletedTask();
        }
    }
}
