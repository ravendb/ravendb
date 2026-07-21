using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Server.Documents.CdcSink.Commands;
using Raven.Server.ServerWide.Commands.CdcSink;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.CdcSink.Test;

internal static class CdcSinkTestProcess
{
    public static async Task<CdcTestResult> VerifyAsync(DocumentDatabase database, CdcSinkConfiguration configuration, CancellationToken ct)
    {
        configuration.SkipInitialLoad = false;
        configuration.TestMode = true;
        
        if (configuration.Connection?.FactoryName == "Npgsql")
        {
            AddCdcSinkCommand.AutoFillPostgresSettings(configuration, Guid.NewGuid().ToString("N"));
        }

        var capture = new CdcSinkTestCapture();

        if (configuration.Validate(out var errors, validateName: false, validateConnection: false) == false)
        {
            capture.SetError(new InvalidOperationException(string.Join(Environment.NewLine, errors)));
            return capture.Result;
        }

        await using (var process = Create(database, configuration, capture))
        {
            await process.RunCdcTestAsync(ct);
        }

        return capture.Result;
    }

    private static ICdcSinkTestProcess Create(DocumentDatabase database, CdcSinkConfiguration configuration, CdcSinkTestCapture capture)
    {
        return configuration.Connection?.FactoryName switch
        {
            "Npgsql" => new TestPostgresCdcSinkProcess(database, configuration, capture),
            "Microsoft.Data.SqlClient" => new TestSqlServerCdcSinkProcess(database, configuration, capture),
            "MySql.Data.MySqlClient" or "MySqlConnector.MySqlConnectorFactory" => new TestMySqlCdcSinkProcess(database, configuration, capture),
            _ => throw new NotSupportedException($"CDC is not supported for provider '{configuration.Connection?.FactoryName}'")
        };
    }

    private sealed class TestSqlServerCdcSinkProcess : SqlServerCdcSinkProcess, ICdcSinkTestProcess
    {
        private readonly CdcSinkTestCapture _capture;

        public TestSqlServerCdcSinkProcess(DocumentDatabase database, CdcSinkConfiguration configuration, CdcSinkTestCapture capture)
            : base(configuration, database) => _capture = capture;

        protected override Task<(string Checkpoint, int Rows)> SubmitBatch(
            List<CdcSinkDocumentOp> ops, string checkpoint,
            Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates,
            CdcSinkBatchCommand.DocumentGrouper grouper)
            => _capture.Handle(ops, checkpoint);

        protected override CdcSinkTaskState LoadState(DocumentsOperationContext context)
            => new() { ConfigurationName = Configuration.Name };

        protected override Task ProcessCdcStream(CancellationToken ct) => Task.CompletedTask;

        protected override async Task<InitialLoadBatch> ReadOneBatch(
            DbConnection conn, CdcSinkConfiguration.TableInfo tableInfo, List<string> keyColumns,
            string[] lastKeys, int maxBatchSize, CancellationToken ct)
        {
            if (_capture.TrySample(tableInfo.FullName) == false)
            {
                return new InitialLoadBatch(new List<CdcSinkDocumentOp>(), lastKeys, null);
            }

            return await base.ReadOneBatch(conn, tableInfo, keyColumns, lastKeys, 1, ct);
        }

        public Task RunCdcTestAsync(CancellationToken ct) => _capture.RunCdcAsync(RunInternalAsync, ct);

        public override async ValueTask DisposeAsync()
        {
            try { await UndoSourceSetupAsync(CancellationToken.None); }
            catch { /* best effort */ }
            await base.DisposeAsync();
        }

        private async Task UndoSourceSetupAsync(CancellationToken ct)
        {
            if (_enabledDatabaseCdc == false && _enabledCaptureTables.Count == 0)
                return;

            await using var conn = await OpenConnectionAsync(ct);

            if (_enabledDatabaseCdc)
            {
                // We enabled CDC on the database, so it had none before us - disabling it reverts every
                // capture instance we added in one step.
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "EXEC sys.sp_cdc_disable_db";
                await cmd.ExecuteNonQueryAsync(ct);
                return;
            }

            // The database already had CDC enabled (not by us) - disable only the capture instances we added.
            foreach (var tableInfo in _enabledCaptureTables)
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "EXEC sys.sp_cdc_disable_table @source_schema = @schema, @source_name = @table, @capture_instance = N'all'";
                AddParameter(cmd, "@schema", tableInfo.Schema);
                AddParameter(cmd, "@table", tableInfo.TableName);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private sealed class TestPostgresCdcSinkProcess : PostgresCdcSinkProcess, ICdcSinkTestProcess
    {
        private readonly CdcSinkTestCapture _capture;

        public TestPostgresCdcSinkProcess(DocumentDatabase database, CdcSinkConfiguration configuration, CdcSinkTestCapture capture)
            : base(configuration, database) => _capture = capture;

        protected override Task<(string Checkpoint, int Rows)> SubmitBatch(
            List<CdcSinkDocumentOp> ops, string checkpoint,
            Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates,
            CdcSinkBatchCommand.DocumentGrouper grouper)
            => _capture.Handle(ops, checkpoint);

        protected override CdcSinkTaskState LoadState(DocumentsOperationContext context)
            => new() { ConfigurationName = Configuration.Name };

        protected override Task ProcessCdcStream(CancellationToken ct) => Task.CompletedTask;

        protected override async Task<InitialLoadBatch> ReadOneBatch(
            DbConnection conn, CdcSinkConfiguration.TableInfo tableInfo, List<string> keyColumns,
            string[] lastKeys, int maxBatchSize, CancellationToken ct)
        {
            if (_capture.TrySample(tableInfo.FullName) == false)
            {
                return new InitialLoadBatch(new List<CdcSinkDocumentOp>(), lastKeys, null);
            }

            return await base.ReadOneBatch(conn, tableInfo, keyColumns, lastKeys, 1, ct);
        }

        public Task RunCdcTestAsync(CancellationToken ct) => _capture.RunCdcAsync(RunInternalAsync, ct);

        public override async ValueTask DisposeAsync()
        {
            try { await UndoSourceSetupAsync(CancellationToken.None); }
            catch { /* best effort */ }
            await base.DisposeAsync();
        }

        private async Task UndoSourceSetupAsync(CancellationToken ct)
        {
            if (_createdSlot == false && _createdPublication == false)
                return;

            await using var conn = _dataSource.CreateConnection();
            await conn.OpenAsync(ct);

            if (_createdSlot)
            {
                await using (var terminate = new NpgsqlCommand(
                                 "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = @slot AND active_pid IS NOT NULL", conn))
                {
                    terminate.Parameters.AddWithValue("slot", _slotName);
                    await terminate.ExecuteScalarAsync(ct);
                }

                await using var dropSlot = new NpgsqlCommand(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = @slot", conn);
                dropSlot.Parameters.AddWithValue("slot", _slotName);
                await dropSlot.ExecuteNonQueryAsync(ct);
            }

            if (_createdPublication)
            {
                await using var dropPub = new NpgsqlCommand(
                    $"DROP PUBLICATION IF EXISTS {CommandBuilder.QuoteIdentifier(_publicationName)}", conn);
                await dropPub.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private sealed class TestMySqlCdcSinkProcess : MySqlCdcSinkProcess, ICdcSinkTestProcess
    {
        private readonly CdcSinkTestCapture _capture;

        public TestMySqlCdcSinkProcess(DocumentDatabase database, CdcSinkConfiguration configuration, CdcSinkTestCapture capture)
            : base(configuration, database) => _capture = capture;

        protected override Task<(string Checkpoint, int Rows)> SubmitBatch(
            List<CdcSinkDocumentOp> ops, string checkpoint,
            Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates,
            CdcSinkBatchCommand.DocumentGrouper grouper)
            => _capture.Handle(ops, checkpoint);

        protected override CdcSinkTaskState LoadState(DocumentsOperationContext context)
            => new() { ConfigurationName = Configuration.Name };

        protected override Task ProcessCdcStream(CancellationToken ct) => Task.CompletedTask;

        protected override async Task<InitialLoadBatch> ReadOneBatch(
            DbConnection conn, CdcSinkConfiguration.TableInfo tableInfo, List<string> keyColumns,
            string[] lastKeys, int maxBatchSize, CancellationToken ct)
        {
            if (_capture.TrySample(tableInfo.FullName) == false)
            {
                return new InitialLoadBatch(new List<CdcSinkDocumentOp>(), lastKeys, null);
            }

            return await base.ReadOneBatch(conn, tableInfo, keyColumns, lastKeys, 1, ct);
        }

        public Task RunCdcTestAsync(CancellationToken ct) => _capture.RunCdcAsync(RunInternalAsync, ct);
    }
}

internal interface ICdcSinkTestProcess : IAsyncDisposable
{
    Task RunCdcTestAsync(CancellationToken ct);
}

internal sealed class CdcSinkTestCapture
{
    private Exception _error;
    private readonly Dictionary<string, string> _sampled = new(StringComparer.Ordinal);

    public bool TrySample(string table) => _sampled.ContainsKey(table) == false;

    public Task<(string Checkpoint, int Rows)> Handle(List<CdcSinkDocumentOp> ops, string checkpoint)
    {
        foreach (var op in ops)
        {
            if (op != null)
                _sampled.Add(op.Processor.FullName, op.DocumentId);
        }

        return Task.FromResult((checkpoint, ops.Count));
    }

    public async Task RunCdcAsync(Func<CancellationToken, Task> runInternal, CancellationToken ct)
    {
        try
        {
            await runInternal(ct);
        }
        catch (Exception e)
        {
            _error = e;
        }
    }

    public void SetError(Exception e) => _error = e;

    public CdcTestResult Result => new()
    {
        Success = _sampled.Count > 0 && _error is null,
        Error = _error?.ToString() ?? (_sampled.Count > 0 ? null : "CDC is configured, but no rows were available to sample."),
        Sampled = _sampled
    };
}
