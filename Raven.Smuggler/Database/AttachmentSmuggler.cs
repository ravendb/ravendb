using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;

namespace Raven.Smuggler.Database
{
    internal class AttachmentSmuggler : SmugglerBase
    {
        public AttachmentSmuggler(DatabaseSmugglerOptions options, DatabaseSmugglerNotifications notifications, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
            : base(options, notifications, source, destination)
        {
        }

        public override async Task SmuggleAsync(DatabaseSmugglerOperationState state, CancellationToken cancellationToken)
        {
            await Source.SkipAttachmentsAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
