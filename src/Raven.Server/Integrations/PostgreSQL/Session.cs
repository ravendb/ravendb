using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class Session
    {
        private readonly TcpClient _client;
        private readonly Func<Stream, Task<(Stream, X509Certificate2)>> _authenticateAsServerIfSslNeeded;
        private readonly int _identifier;
        private readonly int _processId;
        private readonly DatabasesLandlord _databasesLandlord;
        private readonly CancellationToken _token;
        private Dictionary<string, string> _clientOptions;

        public Session(
            TcpClient client,
            int identifier,
            int processId,
            DatabasesLandlord databasesLandlord,
            Func<Stream, Task<(Stream, X509Certificate2)>> authenticateAsServerIfSslNeeded,
            CancellationToken token)
        {
            _client = client;
            _identifier = identifier;
            _processId = processId;
            _databasesLandlord = databasesLandlord;
            _authenticateAsServerIfSslNeeded = authenticateAsServerIfSslNeeded;
            _token = token;
            _clientOptions = null;
        }

        private async Task HandleInitialMessage(Stream stream, PipeReader reader, PipeWriter writer, MessageBuilder messageBuilder)
        {
            var messageReader = new MessageReader();

            var initialMessage = await messageReader.ReadInitialMessage(reader, _token);

            if (initialMessage is SSLRequest)
            {
                X509Certificate2 certificate;

                (stream, certificate) = await _authenticateAsServerIfSslNeeded(stream);

                if (certificate == null)
                {
                    await writer.WriteAsync(messageBuilder.SSLResponse(false), _token);

                    await reader.CompleteAsync();
                    initialMessage = await messageReader.ReadInitialMessage(reader, _token);
                }
                else
                {
                    await writer.WriteAsync(messageBuilder.SSLResponse(true), _token);
                    var encryptedReader = PipeReader.Create(stream);
                    initialMessage = await messageReader.ReadInitialMessage(encryptedReader, _token);
                }
            }

            switch (initialMessage)
            {
                case StartupMessage startupMessage:
                    _clientOptions = startupMessage.ClientOptions;
                    break;
                case SSLRequest:
                    await writer.WriteAsync(messageBuilder.ErrorResponse(
                        PgSeverity.Fatal,
                        PgErrorCodes.ProtocolViolation,
                        "SSLRequest received twice"), _token);
                    return;
                case Cancel cancel: 
                    // TODO: Support Cancel message
                    await writer.WriteAsync(messageBuilder.ErrorResponse(
                        PgSeverity.Fatal,
                        PgErrorCodes.FeatureNotSupported,
                        "Cancel message support not implemented."), _token);
                    return;
                default:
                    await writer.WriteAsync(messageBuilder.ErrorResponse(
                        PgSeverity.Fatal,
                        PgErrorCodes.ProtocolViolation,
                        "Invalid first message received"), _token);
                    return;
            }
        }

        public async Task Run()
        {
            using var _ = _client;
            using var messageBuilder = new MessageBuilder();
            await using var stream = _client.GetStream();

            var reader = PipeReader.Create(stream);
            var writer = PipeWriter.Create(stream);

            await HandleInitialMessage(stream, reader, writer, messageBuilder);

            if (_clientOptions == null) //TODO pfyasu maybe unused when cancel message will be implemented
                return;

            if (_clientOptions.TryGetValue("database", out string databaseName) == false)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Fatal,
                    PgErrorCodes.ConnectionFailure,
                    "Failed to connect to database",
                    "Missing database name in the connection string"), _token);
                return;
            }

            var database = await _databasesLandlord.TryGetOrCreateResourceStore(databaseName);

            if (database == null)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Fatal,
                    PgErrorCodes.ConnectionFailure,
                    "Failed to connect to database",
                    $"Database '{databaseName}' does not exist"), _token);
                return;
            }

            try
            {
                using var transaction = new Transaction(database, new MessageReader(), _clientOptions["user"]);

                // Authentication
                await writer.WriteAsync(messageBuilder.AuthenticationCleartextPassword(), _token);
                var authMessage = await transaction.MessageReader.GetUninitializedMessage(reader, _token);
                await authMessage.Init(transaction.MessageReader, reader, _token);
                await authMessage.Handle(transaction, messageBuilder, reader, writer, _token);

                await writer.WriteAsync(messageBuilder.ParameterStatusMessages(PgConfig.ParameterStatusList), _token);
                await writer.WriteAsync(messageBuilder.BackendKeyData(_processId, _identifier), _token);
                await writer.WriteAsync(messageBuilder.ReadyForQuery(transaction.State), _token);

                while (_token.IsCancellationRequested == false)
                {
                    var message = await transaction.MessageReader.GetUninitializedMessage(reader, _token);

                    try
                    {
                        await message.Init(transaction.MessageReader, reader, _token);
                        await message.Handle(transaction, messageBuilder, reader, writer, _token);
                    }
                    catch (PgErrorException e)
                    {
                        await message.HandleError(e, transaction, messageBuilder, writer, _token);
                    }
                }
            }
            catch (PgFatalException e)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Fatal,
                    e.ErrorCode,
                    e.Message,
                    e.ToString()), _token);
            }
            catch (PgErrorException e)
            {
                // Shouldn't get to this point, PgErrorExceptions shouldn't be fatal
                await writer.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Error,
                    e.ErrorCode,
                    e.Message,
                    e.ToString()), _token);
            }
            catch (PgTerminateReceivedException)
            {
                // Terminate silently
            }
            catch (Exception e)
            {
                try
                {
                    await writer.WriteAsync(messageBuilder.ErrorResponse(
                        PgSeverity.Fatal,
                        PgErrorCodes.InternalError,
                        e.ToString()), _token);
                }
                catch (Exception)
                {
                    // ignored
                }
            }
        }
    }
}
