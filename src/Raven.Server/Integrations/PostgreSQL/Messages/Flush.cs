using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class Flush : Message
    {
        protected override Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token)
        {
            return Task.FromResult(0);
        }

        protected override async Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            await writer.FlushAsync(token);
        }
    }
}
