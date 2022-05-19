using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Operations;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations;

public class ShardedOperation : AbstractOperation
{
    [JsonDeserializationIgnore]
    public MultiOperation Operation;

    public override async Task KillAsync(bool waitForCompletion, CancellationToken token)
    {
        if (Operation != null)
            await Operation.KillAsync(waitForCompletion, token);

        await base.KillAsync(waitForCompletion, token);
    }
}
