using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class Close : ExtendedProtocolMessage
    {
        public PgObjectType PgObjectType;
        public string ObjectName;

        protected override async Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token)
        {
            var len = 0;

            var objectType = await messageReader.ReadByteAsync(reader, token);
            len += sizeof(byte);

            var pgObjectType = objectType switch
            {
                (byte)PgObjectType.Portal => PgObjectType.Portal,
                (byte)PgObjectType.PreparedStatement => PgObjectType.PreparedStatement,
                _ => throw new PgFatalException(PgErrorCodes.ProtocolViolation,
                    "Expected valid object type ('S' or 'P') but got: '" + objectType)
            };

            var (objectName, objectNameLength) = await messageReader.ReadNullTerminatedString(reader, token);
            len += objectNameLength;

            PgObjectType = pgObjectType;
            ObjectName = objectName;

            return len;
        }

        protected override async Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            transaction.Close();
            await writer.WriteAsync(messageBuilder.CloseComplete(), token);
        }
    }
}
