using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL
{
    public sealed class PgSession
    {
        private static RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<PgSession>();
        internal ConcurrentDictionary<string, PgQuery> NamedStatements { get; private set; }
        private readonly TcpClient _client;
        private readonly CertificateUtils.CertificateHolder _serverCertificateHolder;
        private readonly int _identifier;
        private readonly int _processId;
        private readonly ServerStore _serverStore;
        private readonly CancellationToken _token;
        private Dictionary<string, string> _clientOptions;
        // The active handshake stream: the raw TcpClient stream before the TLS upgrade, the SslStream
        // after. A field so Run()'s error catch can write the
        // ErrorResponse on the correct stream even if the post-upgrade parse throws.
        private Stream _activeHandshakeStream;

        public PgSession(
            TcpClient client,
            CertificateUtils.CertificateHolder serverCertificateHolder,
            int identifier,
            int processId,
            ServerStore serverStore,
            CancellationToken token)
        {
            _client = client;
            _serverCertificateHolder = serverCertificateHolder;
            _identifier = identifier;
            _processId = processId;
            _serverStore = serverStore;
            _token = token;
            _clientOptions = null;
            NamedStatements = new ConcurrentDictionary<string, PgQuery>();
        }

        private async Task<Stream> HandleInitialMessage(Stream stream, MessageBuilder messageBuilder)
        {
            _activeHandshakeStream = stream;

            var reader = PipeReader.Create(stream);
            var writer = PipeWriter.Create(stream);

            var streamToUse = stream;

            var messageReader = new MessageReader();

            var initialMessage = await messageReader.ReadInitialMessage(reader, _token);

            if (initialMessage is SSLRequest)
            {
                if (_serverCertificateHolder.ServerCertificate == null)
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
                        ServerCertificateContext = _serverCertificateHolder.ServerCertificateContext,
                        ClientCertificateRequired = false
                    }, _token);

                    streamToUse = sslStream;
                    // Track the upgrade so Run()'s catch can send any post-upgrade ErrorResponse
                    // on the SslStream rather than the raw NetworkStream. Without this, a fatal
                    // exception from the post-TLS StartupMessage parse would emit unencrypted
                    // bytes on a channel the client now treats as TLS-encrypted.
                    _activeHandshakeStream = sslStream;

                    var encryptedReader = PipeReader.Create(sslStream);
                    initialMessage = await messageReader.ReadInitialMessage(encryptedReader, _token);
                }
            }

            switch (initialMessage)
            {
                case StartupMessage startupMessage:
                    _clientOptions = startupMessage.ClientOptions;
                    WarnIfUnsupportedClientEncoding(_clientOptions);
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

        // We always operate in and report UTF8 (see PgConfig.ParameterStatusList) and cannot transcode.
        // A client may still declare a different client_encoding in its startup packet. We deliberately
        // accept it rather than reject the connection - pgAdmin / PowerBI / Microsoft Fabric rely on
        // connecting, and a spec-compliant client honours the UTF8 we report back in ParameterStatus.
        // We log the mismatch so that, if a client ignores the reported encoding and mis-decodes results,
        // the cause is diagnosable instead of silent.
        private static void WarnIfUnsupportedClientEncoding(Dictionary<string, string> clientOptions)
        {
            if (Logger.IsInfoEnabled == false)
                return;
            if (clientOptions == null || clientOptions.TryGetValue("client_encoding", out var encoding) == false || string.IsNullOrEmpty(encoding))
                return;

            // Accept the common UTF8 spellings (UTF8, utf-8, UNICODE) without logging noise.
            var normalized = encoding.Replace("-", "").Replace("_", "");
            if (normalized.Equals("UTF8", StringComparison.OrdinalIgnoreCase) || normalized.Equals("UNICODE", StringComparison.OrdinalIgnoreCase))
                return;

            Logger.Info($"PostgreSQL client requested client_encoding='{encoding}', which is not supported. " +
                        "The server reports and emits UTF8; a client that ignores the reported encoding may mis-decode results.");
        }

        public async Task Run()
        {
            using var _ = _client;
            using var messageBuilder = new MessageBuilder();

            Stream stream = _client.GetStream();

            // A writer so a handshake/StartupMessage parse failure sends an ErrorResponse on the
            // active stream (_activeHandshakeStream), not a bare socket close or plaintext post-TLS.
            try
            {
                stream = await HandleInitialMessage(stream, messageBuilder);
            }
            catch (PgFatalException e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Initial handshake failed: {e.Message} (pg error code {e.ErrorCode}).", e);

                await TryWriteHandshakeErrorResponse(stream, messageBuilder, e.ErrorCode, e.Message, e.ToString());
                return;
            }
            catch (OperationCanceledException)
            {
                // Server shutdown or TCP listener cancelled mid-handshake - nothing to log, no
                // useful diagnostic to send (the cancel is ours, not the client's fault).
                return;
            }
            catch (Exception e) when (e is IOException or EndOfStreamException)
            {
                // Network failure during handshake (peer closed, timeout, TLS handshake aborted).
                // The socket is almost certainly unwritable, but log so we have forensics - otherwise
                // Run() exits silently and the client sees only a closed socket.
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Initial handshake aborted: {e.GetType().Name}: {e.Message}.", e);
                return;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Unexpected internal error during initial handshake.", e);

                await TryWriteHandshakeErrorResponse(stream, messageBuilder, PgErrorCodes.InternalError, "Internal error during handshake.", e.ToString());
                return;
            }

            var reader = PipeReader.Create(stream);
            var writer = PipeWriter.Create(stream);

            if (_clientOptions == null)
                return;

            // Security gate: in secured mode, refuse a plaintext (non-TLS) socket before any database
            // lookup, so credentials and database existence are never probed over an unencrypted channel.
            if (_serverCertificateHolder.ServerCertificate != null && stream is not SslStream)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Fatal,
                    PgErrorCodes.InvalidAuthorizationSpecification,
                    "This server requires a TLS connection for authentication. " +
                    "Configure your client to use SSL (e.g. Npgsql 'SSL Mode=Require' " +
                    "or 'SSL Mode=Prefer') so the password is encrypted in transit."), _token);
                return;
            }

            if (_clientOptions.TryGetValue("database", out string databaseName) == false)
            {
                await writer.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Fatal,
                    PgErrorCodes.ConnectionFailure,
                    "Failed to connect to database",
                    "Missing database name in the connection string"), _token);
                return;
            }

            var result = _serverStore.DatabasesLandlord.TryGetOrCreateDatabase(databaseName);

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

                if (_serverCertificateHolder.ServerCertificate != null)
                {
                    // Authentication is required only when running in secured mode. The plaintext
                    // TLS gate above already refused any non-SslStream connection in secured mode,
                    // so reaching here guarantees the cleartext password travels encrypted.
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
                        if (TrafficWatchManager.HasRegisteredClients && message is Query queryMessage)
                            DispatchPostgresQueryMessageToTrafficWatch(queryMessage);
                    }
                    catch (PgErrorException e)
                    {
                        if (TrafficWatchManager.HasRegisteredClients && message is Query queryMessage)
                            DispatchPostgresQueryMessageToTrafficWatch(queryMessage, e);
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

        // Best-effort: write a Fatal ErrorResponse on whichever stream the handshake was using
        // when the failure occurred (NetworkStream pre-TLS-upgrade, SslStream post-upgrade). The
        // socket may already be unwritable, in which case we swallow - at this point we've
        // already logged and there's nothing else to surface.
        private async Task TryWriteHandshakeErrorResponse(Stream stream, MessageBuilder messageBuilder, string errorCode, string message, string detail)
        {
            try
            {
                var streamForError = _activeHandshakeStream ?? stream;
                var earlyWriter = PipeWriter.Create(streamForError);
                await earlyWriter.WriteAsync(messageBuilder.ErrorResponse(
                    PgSeverity.Fatal,
                    errorCode,
                    message,
                    detail), _token);
            }
            catch
            {
                // best-effort: socket may already be unwritable
            }
        }

        private string GetSourceConnectionDetails(string userName)
        {
            var details = $" Source connection details - IP: {_client.Client.LocalEndPoint}";

            if (string.IsNullOrEmpty(userName) == false)
                details += $" - Username: {userName}";

            return details;
        }
        
        private void DispatchPostgresQueryMessageToTrafficWatch(Query message, PgErrorException e = null)
        {
            var clientIp = _client.Client.RemoteEndPoint?.ToString();
            string databaseName = _clientOptions.GetValueOrDefault("database", "N/A");
            string username = _clientOptions.GetValueOrDefault("user", "N/A");

            var twn = new TrafficWatchPostgresChange()
            {
                TimeStamp = DateTime.UtcNow,
                DatabaseName = databaseName,  
                CertificateThumbprint = null,
                CustomInfo = e is null ? null : $"{e.ErrorCode} - {e.Message}",
                ClientIP = clientIp,
                Source = _serverStore.NodeTag,
                Username = username,
                Query = message.QueryString
            };
            TrafficWatchManager.DispatchMessage(twn);
    }
}
}
