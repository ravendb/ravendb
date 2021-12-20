using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public abstract class ExtendedProtocolMessage : Message
    {
        public override async Task Handle(PgTransaction transaction, MessageBuilder messageBuilder, PipeReader reader, PipeWriter writer, CancellationToken token)
        {
            if (transaction.State == TransactionState.Failed && this is not Sync)
                return;

            await base.Handle(transaction, messageBuilder, reader, writer, token);
        }

        public override async Task HandleError(PgErrorException e, PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            transaction.Fail();
            await base.HandleError(e, transaction, messageBuilder, writer, token);
        }
    }
}
