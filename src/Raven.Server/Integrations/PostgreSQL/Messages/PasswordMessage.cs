using System;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class PasswordMessage : Message
    {
        public string Password;

        private const string ErrorSeverity = "FATAL";
        private const string InvalidRoleSpecification = "0P000";
        private const string InvalidPasswordErrorCode = "28P01";

        private string PasswordAuthFailedErrorMessage(string username) => $"password authentication failed for user \"{username}\"";

        private string RoleDoesNotExistErrorMessage(string username) => $"role \"{username}\" does not exist";

        protected override async Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            var serverStore = transaction.DocumentDatabase.ServerStore;
            var databaseName = transaction.DocumentDatabase.Name;

            DatabaseRecord databaseRecord;
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
                databaseRecord = serverStore.Cluster.ReadDatabase(context, databaseName, out long index);

            var users = databaseRecord?.Integrations?.PostgreSql?.Authentication?.Users;

            var user = users?.SingleOrDefault(x => x.Username.Equals(transaction.Username, StringComparison.OrdinalIgnoreCase));

            if (user == null)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(ErrorSeverity, InvalidRoleSpecification, RoleDoesNotExistErrorMessage(transaction.Username)), token);
                return;
            }
            
            if (Password.Equals(user.Password) == false)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(ErrorSeverity, InvalidPasswordErrorCode, PasswordAuthFailedErrorMessage(transaction.Username)), token);
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
