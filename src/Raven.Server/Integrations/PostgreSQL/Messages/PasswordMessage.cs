using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class PasswordMessage : Message
    {
        public string Password;

        protected override async Task HandleMessage(Transaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            //if (!Password.Equals("12345678"))
            //{
                //throw new PgFatalException(PgErrorCodes.InvalidPassword, "Authentication failed, password is invalid.");
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
