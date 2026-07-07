using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public sealed class Query : Message
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
            // Simple Query Protocol: a single message can carry multiple `;`-separated statements,
            // each producing its own result set (RowDescription + DataRow* + CommandComplete).
            // Only ONE ReadyForQuery at the end. Single-statement inputs go through the same loop
            // — the splitter returns a one-element list.
            var statements = SqlStatementSplitter.Split(QueryString);
            if (statements.Count == 0)
                statements.Add(QueryString); // empty / whitespace-only — let CreateInstance handle it.

            foreach (var stmt in statements)
            {
                using var query = PgQuery.CreateInstance(stmt, null, transaction.DocumentDatabase, transaction.Session, transaction.Username);

                var schema = await query.Init();
                if (schema != null && schema.Count != 0)
                {
                    await writer.WriteAsync(messageBuilder.RowDescription(schema), token);
                }

                await query.Execute(messageBuilder, writer, token);
            }

            await writer.WriteAsync(messageBuilder.ReadyForQuery(transaction.State), token);
        }

        public override async Task HandleError(PgErrorException e, PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            await base.HandleError(e, transaction, messageBuilder, writer, token);
            await writer.WriteAsync(messageBuilder.ReadyForQuery(transaction.State), token);
        }
    }
}
