using System.IO.Pipelines;
using System.Linq;
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

        private readonly string _errorSeverity = "FATAL";
        private readonly string _errorCode = "28P01";
        private string ErrorMessage(string username) => $"password authentication failed for user \"{username}\"";

        protected override async Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            var serverStore = transaction.DocumentDatabase.ServerStore;
            var databaseName = transaction.DocumentDatabase.Name;

            DatabaseRecord databaseRecord;
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
                databaseRecord = serverStore.Cluster.ReadDatabase(context, databaseName, out long index);

            var users = databaseRecord?.Integrations?.PostgreSql?.Authentication?.Users;

            if (users == null)
                throw new PgFatalException(PgErrorCodes.NoDataFound, "Authentication failed");

            var user = users
                .SingleOrDefault(x => x.Username == transaction.Username);

            if (user == null || Password.Equals(user.Password) == false)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(_errorSeverity, _errorCode, ErrorMessage(transaction.Username)), token);
                return;
            }

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
