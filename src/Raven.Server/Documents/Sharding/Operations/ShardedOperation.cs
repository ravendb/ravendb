using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Operations;

namespace Raven.Server.Documents.Sharding.Operations;

public class ShardedOperation : AbstractOperation
{
    public MultiOperation Operation;

    public override async ValueTask KillAsync(bool waitForCompletion, CancellationToken token)
    {
        if (Operation != null)
            await Operation.KillAsync(waitForCompletion, token);

        await base.KillAsync(waitForCompletion, token);
    }
}
