using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class PasswordMessage : Message
    {
        public string Password;

        protected override async Task HandleMessage(Transaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            var serverStore = transaction.DocumentDatabase.ServerStore;
            var databaseName = transaction.DocumentDatabase.Name;

            DatabaseRecord databaseRecord;
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
                databaseRecord = serverStore.Cluster.ReadDatabase(context, databaseName, out long index);

            var result = databaseRecord.Integrations.PostgreSql.Authentication.Users;

            // TODO arek
            //var password = result[transaction.Username];
            //if (Password.Equals(password) == false)
            //{
            //    throw new PgFatalException(PgErrorCodes.InvalidPassword, "Authentication failed, password is invalid.");
            //}

            await writer.WriteAsync(messageBuilder.AuthenticationOk(), token);
        }

        protected override async Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token)
        {
            var len = 0;

            var (password, passwordLength) = await messageReader.ReadNullTerminatedString(reader, token);
            len += passwordLength;

            Password = password;

            return len;
        }
    }
}
