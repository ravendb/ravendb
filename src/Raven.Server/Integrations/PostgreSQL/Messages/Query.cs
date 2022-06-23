using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class Query : Message
    {
        public string QueryString;

        protected override async Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token)
        {
            var len = 0;

            var (queryString, queryStringLength) = await messageReader.ReadNullTerminatedString(reader, token);
            len += queryStringLength;

            QueryString = queryString;

            return len;
        }

        protected override async Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            // TODO: Maybe support multiple SELECT statements in one query - requires parsing the SQL
            using var query = PgQuery.CreateInstance(QueryString, null, transaction.DocumentDatabase, transaction.Session);

            var schema = await query.Init(true);
            if (schema.Count != 0)
            {
                await writer.WriteAsync(messageBuilder.RowDescription(schema), token);
            }

            await query.Execute(messageBuilder, writer, token);
            await writer.WriteAsync(messageBuilder.ReadyForQuery(transaction.State), token);
        }

        public override async Task HandleError(PgErrorException e, PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            await base.HandleError(e, transaction, messageBuilder, writer, token);
            await writer.WriteAsync(messageBuilder.ReadyForQuery(transaction.State), token);
        }
    }
}
