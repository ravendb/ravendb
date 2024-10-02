using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Integrations.PostgreSQL
{
    public sealed class PgSession
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<PgSession>("Postgres Server");
        internal ConcurrentDictionary<string, PgQuery> NamedStatements { get; private set; }
        private readonly TcpClient _client;
        private readonly CertificateUtils.CertificateHolder _serverCertificateHolder;
        private readonly int _identifier;
        private readonly int _processId;
        private readonly DatabasesLandlord _databasesLandlord;
        private readonly CancellationToken _token;
        private Dictionary<string, string> _clientOptions;

        public PgSession(
            TcpClient client,
            CertificateUtils.CertificateHolder serverCertificateHolder,
            int identifier,
            int processId,
            DatabasesLandlord databasesLandlord,
            CancellationToken token)
        {
            _client = client;
            _serverCertificateHolder = serverCertificateHolder;
            _identifier = identifier;
            _processId = processId;
            _databasesLandlord = databasesLandlord;
            _token = token;
            _clientOptions = null;
            NamedStatements = new ConcurrentDictionary<string, PgQuery>();
        }

        private async Task<Stream> HandleInitialMessage(Stream stream, MessageBuilder messageBuilder)
        {
            var reader = PipeReader.Create(stream);
            var writer = PipeWriter.Create(stream);

            var streamToUse = stream;

            var messageReader = new MessageReader();

            var initialMessage = await messageReader.ReadInitialMessage(reader, _token);

            if (initialMessage is SSLRequest)
            {
                if (_serverCertificateHolder.Certificate == null)
                {
                    await writer.WriteAsync(messageBuilder.SSLResponse(false), _token);
                    initialMessage = await messageReader.ReadInitialMessage(reader, _token);
                }
                else
                {
                    await writer.WriteAsync(messageBuilder.SSLResponse(true), _token);
                    var sslStream = new SslStream(stream, false, (sender, certificate, chain, errors) => true);

                    await sslStream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
                    {
                        ServerCertificateContext = _serverCertificateHolder.CertificateContext,
                        ClientCertificateRequired = false
                    }, _token);

                    streamToUse = sslStream;

                    var encryptedReader = PipeReader.Create(sslStream);
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
                    break;
                case Cancel cancel:
                    // TODO: Support Cancel message
                    await writer.WriteAsync(messageBuilder.ErrorResponse(
                        PgSeverity.Fatal,
                        PgErrorCodes.FeatureNotSupported,
                        "Cancel message support not implemented."), _token);
                    break;
                default:
                    await writer.WriteAsync(messageBuilder.ErrorResponse(
                        PgSeverity.Fatal,
                        PgErrorCodes.ProtocolViolation,
                        "Invalid first message received"), _token);
                    break;
            }

            return streamToUse;
        }

        public async Task Run()
        {
            using var _ = _client;
            using var messageBuilder = new MessageBuilder();

            Stream stream = _client.GetStream();

            stream = await HandleInitialMessage(stream, messageBuilder);

            var reader = PipeReader.Create(stream);
            var writer = PipeWriter.Create(stream);

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

            var result = _databasesLandlord.TryGetOrCreateDatabase(databaseName);

            if (result.DatabaseStatus == DatabasesLandlord.DatabaseSearchResult.Status.Missing)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Fatal,
                    PgErrorCodes.ConnectionFailure,
                    "Failed to connect to database",
                    $"Database '{databaseName}' does not exist"), _token);
                return;
            }

            if (result.DatabaseStatus == DatabasesLandlord.DatabaseSearchResult.Status.Sharded)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Fatal,
                    PgErrorCodes.ConnectionFailure,
                    "Failed to connect to database",
                    $"Database '{databaseName}' is a sharded database and does not support PostgreSQL."), _token);
                return;
            }

            var database = await result.DatabaseTask;

            string username = null;

            try
            {
                username = _clientOptions["user"];

                using var transaction = new PgTransaction(database, new MessageReader(), username, this);

                if (_serverCertificateHolder.Certificate != null)
                {
                    // Authentication is required only when running in secured mode

                    await writer.WriteAsync(messageBuilder.AuthenticationCleartextPassword(), _token);
                    var authMessage = await transaction.MessageReader.GetUninitializedMessage(reader, _token);
                    await authMessage.Init(transaction.MessageReader, reader, _token);
                    await authMessage.Handle(transaction, messageBuilder, reader, writer, _token);
                }
                else
                {
                    await writer.WriteAsync(messageBuilder.AuthenticationOk(), _token);
                }

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
                if (Logger.IsInfoEnabled)
                    Logger.Info($"{e.Message} (fatal pg error code {e.ErrorCode}). {GetSourceConnectionDetails(username)}", e);

                await writer.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Fatal,
                    e.ErrorCode,
                    e.Message,
                    e.ToString()), _token);
            }
            catch (PgErrorException e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"{e.Message} (pg error code {e.ErrorCode}). {GetSourceConnectionDetails(username)}", e);

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
            catch (QueryParser.ParseException e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Invalid RQL query", e);

                try
                {
                    await writer.WriteAsync(messageBuilder.ErrorResponse(
                        PgSeverity.Error,
                        PgErrorCodes.InvalidSqlStatementName,
                        e.ToString()), _token);
                }
                catch (Exception)
                {
                    // ignored
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Unexpected internal pg error. {GetSourceConnectionDetails(username)}", e);

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

        private string GetSourceConnectionDetails(string userName)
        {
            var details = $" Source connection details - IP: {_client.Client.LocalEndPoint}";

            if (string.IsNullOrEmpty(userName) == false)
                details += $" - Username: {userName}";

            return details;
        }
    }
}
