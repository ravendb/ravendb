using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class Bind : ExtendedProtocolMessage
    {
        public string PortalName;
        public string StatementName;
        public short[] ParameterFormatCodes;
        public List<byte[]> Parameters;
        public short[] ResultColumnFormatCodes;

        protected override async Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token)
        {
            var len = 0;

            var (destPortalName, destPortalLength) = await messageReader.ReadNullTerminatedString(reader, token);
            len += destPortalLength;

            var (preparedStatementName, preparedStatementLength) = await messageReader.ReadNullTerminatedString(reader, token);
            len += preparedStatementLength;

            var parameterFormatCodeCount = await messageReader.ReadInt16Async(reader, token);
            len += sizeof(short);

            var parameterCodes = new short[parameterFormatCodeCount];
            for (var i = 0; i < parameterFormatCodeCount; i++)
            {
                parameterCodes[i] = await messageReader.ReadInt16Async(reader, token);
                len += sizeof(short);
            }

            var parametersCount = await messageReader.ReadInt16Async(reader, token);
            len += sizeof(short);

            var parameters = new List<byte[]>(parametersCount);
            for (var i = 0; i < parametersCount; i++)
            {
                var parameterLength = await messageReader.ReadInt32Async(reader, token);
                len += sizeof(int);

                parameters.Add(await messageReader.ReadBytesAsync(reader, parameterLength, token));
                len += parameterLength;
            }

            var resultColumnFormatCodesCount = await messageReader.ReadInt16Async(reader, token);
            len += sizeof(short);

            var resultColumnFormatCodes = new short[resultColumnFormatCodesCount];
            for (var i = 0; i < resultColumnFormatCodesCount; i++)
            {
                resultColumnFormatCodes[i] = await messageReader.ReadInt16Async(reader, token);
                len += sizeof(short);
            }

            PortalName = destPortalName;
            StatementName = preparedStatementName;
            ParameterFormatCodes = parameterCodes;
            Parameters = parameters;
            ResultColumnFormatCodes = resultColumnFormatCodes;

            return len;
        }

        protected override async Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            if (ParameterFormatCodes.Length != Parameters.Count &&
                ParameterFormatCodes.Length != 0 &&
                ParameterFormatCodes.Length != 1)
            {
                throw new PgErrorException(PgErrorCodes.ProtocolViolation,
                    $"Parameter format code amount is {ParameterFormatCodes.Length} when expected " +
                    $"to be 0, 1 or equal to the parameters count {Parameters.Count}.");
            }

            transaction.Bind(Parameters, ParameterFormatCodes, ResultColumnFormatCodes, StatementName);
            await writer.WriteAsync(messageBuilder.BindComplete(), token);
        }
    }
}
