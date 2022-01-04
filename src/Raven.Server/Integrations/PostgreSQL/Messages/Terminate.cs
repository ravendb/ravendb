using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class Terminate : Message
    {
        protected override Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token)
        {
            return Task.FromResult(0);
        }

        protected override Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            throw new PgTerminateReceivedException();
        }
    }
}
