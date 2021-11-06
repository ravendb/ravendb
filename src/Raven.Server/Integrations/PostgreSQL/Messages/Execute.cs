using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class Execute : ExtendedProtocolMessage
    {
        public string PortalName;
        public int MaxRows;

        protected override async Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token)
        {
            var len = 0;

            var (portalName, portalNameLength) = await messageReader.ReadNullTerminatedString(reader, token);
            len += portalNameLength;

            var maxRowsToReturn = await messageReader.ReadInt32Async(reader, token);
            len += sizeof(int);

            PortalName = portalName;
            MaxRows = maxRowsToReturn;

            return len;
        }

        protected override async Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            if (transaction.State == TransactionState.Idle)
                throw new PgErrorException(PgErrorCodes.NoActiveSqlTransaction,
                    "Execute message was received when no transaction is taking place.");

            await transaction.Execute(messageBuilder, writer, token);
        }
    }
}
