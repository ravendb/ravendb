using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Data.SqlClient;
using Npgsql;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26378 : SqlAwareTestBase
{
    public RavenDB_26378(ITestOutputHelper output) : base(output)
    {
    }

    // Once the underlying SQL connection/transaction is broken (e.g. KILL on the server, network drop)
    // every subsequent insert in the same ETL batch is doomed. Before the fix the writer kept iterating
    // and recorded a partial-load error per remaining row in the batch:
    //   - Microsoft.Data.SqlClient: "ExecuteNonQuery requires an open and available Connection."
    //   - Npgsql:                   "Transaction is already completed"
    // After the fix the writer detects the dead state and aborts the batch immediately, so only the
    // primary connection-drop error is recorded - the secondary symptom strings should never appear.
    //
    // Coverage note: these tests exercise the insert path. The fix also bails out of DeleteItems via
    // the same EnsureTargetConnectionAlive helper - delete coverage is implicit through the shared base.

    private static readonly string[] SecondarySymptoms =
    {
        "ExecuteNonQuery requires an open and available Connection",
        "Transaction is already completed",
        "This SqlTransaction has completed",
    };

    [RavenRetryFact(RavenTestCategory.Etl, maxRetries: 3, delayBetweenRetriesMs: 1000, Requires = RavenServiceRequirement.MsSql)]
    public Task ConnectionDropMidBatch_ShouldStopBatchInsteadOfFloodingErrors_MsSql() =>
        RunConnectionDropTest(
            provider: MigrationProvider.MsSQL,
            factoryName: "Microsoft.Data.SqlClient",
            createTable: cs =>
            {
                using var con = new SqlConnection(cs);
                con.Open();
                using var cmd = con.CreateCommand();
                cmd.CommandText = @"
CREATE TABLE [dbo].[Users]
(
    [Id]   [nvarchar](100) NOT NULL,
    [Name] [nvarchar](100) NULL
)";
                cmd.ExecuteNonQuery();
            },
            killSessionsOnce: cs => KillMsSqlSessions(cs));

    [RavenRetryFact(RavenTestCategory.Etl, maxRetries: 3, delayBetweenRetriesMs: 1000, Requires = RavenServiceRequirement.NpgSql)]
    public Task ConnectionDropMidBatch_ShouldStopBatchInsteadOfFloodingErrors_Postgres() =>
        RunConnectionDropTest(
            provider: MigrationProvider.NpgSQL,
            factoryName: "Npgsql",
            createTable: cs =>
            {
                using var con = new NpgsqlConnection(cs);
                con.Open();
                using var cmd = con.CreateCommand();
                cmd.CommandText = @"
CREATE TABLE ""Users""
(
    ""Id""   varchar(100) NOT NULL,
    ""Name"" varchar(100) NULL
)";
                cmd.ExecuteNonQuery();
            },
            killSessionsOnce: cs => KillPostgresBackends(cs));

    private async Task RunConnectionDropTest(
        MigrationProvider provider,
        string factoryName,
        Action<string> createTable,
        Func<string, int> killSessionsOnce)
    {
        const int docsCount = 5000;

        using (var store = GetDocumentStore())
        using (WithSqlDatabase(provider, out var connectionString, out _, dataSet: null, includeData: false))
        {
            createTable(connectionString);

            using (var bulkInsert = store.BulkInsert())
            {
                for (var i = 0; i < docsCount; i++)
                    await bulkInsert.StoreAsync(new User { Name = "User-" + i });
            }

            var loadErrorObserved = Etl.WaitForEtlToComplete(store, (_, s) => s.LastAlert != null);

            // Schedule a single, well-timed kill mid-batch. Killing in a loop is overkill (one drop is
            // enough to surface the bug) and causes SQL Server to start rejecting connection attempts
            // after enough rapid kill churn.
            using var killerCts = new CancellationTokenSource();
            var killer = ScheduleSingleKill(connectionString, killSessionsOnce, killerCts.Token, msg => Output.WriteLine(msg));

            SetupSqlEtl(store, connectionString, factoryName);

            var observed = await loadErrorObserved.WaitAsync(TimeSpan.FromMinutes(2));
            Assert.True(observed, "Did not observe a load-error alert within 2 minutes - the killer may not have landed in time.");

            killerCts.Cancel();
            try { await killer; } catch { /* ignore */ }

            var database = await GetDatabase(store.Database);
            var etlProcess = database.EtlLoader.Processes.Single();
            var alert = etlProcess.Statistics.LastAlert;
            Assert.NotNull(alert);
            var details = Assert.IsType<EtlErrorsDetails>(alert.Details);
            var errors = details.Errors.ToList();

            Assert.NotEmpty(errors);

            // Anti-regression: the writer must not march on through a dead transaction. The strings
            // below only show up when CreateCommand or ExecuteNonQuery is called on a connection/transaction
            // that the previous failed insert already left in a broken state.
            var leaked = errors
                .Where(e => SecondarySymptoms.Any(s => e.Error.Contains(s, StringComparison.OrdinalIgnoreCase)))
                .ToList();

            Assert.True(leaked.Count == 0,
                $"Expected the writer to stop on the first connection-drop error and surface only the underlying " +
                $"connection failure. Found {leaked.Count} downstream error(s) recorded after the connection " +
                $"was already dead. Sample:" + Environment.NewLine +
                string.Join(Environment.NewLine + "----" + Environment.NewLine,
                    leaked.Take(3).Select(e => e.Error)));
        }
    }

    private static Task ScheduleSingleKill(string connectionString, Func<string, int> killOnce, CancellationToken token, Action<string> log)
    {
        return Task.Run(async () =>
        {
            // Poll for the ETL session to appear, then kill once. We don't have a direct signal that
            // a transaction has started, so we keep retrying until at least one matching session shows
            // up - bounded by the test-level timeout and the cancellation token.
            while (token.IsCancellationRequested == false)
            {
                try
                {
                    var killed = killOnce(connectionString);
                    if (killed > 0)
                    {
                        log($"[killer] KILLed {killed} session(s)");
                        return;
                    }
                }
                catch (Exception e)
                {
                    log($"[killer] error: {e.Message}");
                }

                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }, token);
    }

    private static int KillMsSqlSessions(string connectionString)
    {
        using var con = new SqlConnection(connectionString);
        con.Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText = @"
DECLARE @killed INT = 0;
DECLARE @sid INT;
DECLARE @sql NVARCHAR(50);
DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT session_id FROM sys.dm_exec_sessions
    WHERE database_id = DB_ID() AND session_id <> @@SPID;
OPEN c;
FETCH NEXT FROM c INTO @sid;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'KILL ' + CAST(@sid AS NVARCHAR(10));
    BEGIN TRY EXEC sp_executesql @sql; SET @killed = @killed + 1; END TRY BEGIN CATCH END CATCH;
    FETCH NEXT FROM c INTO @sid;
END;
CLOSE c; DEALLOCATE c;
SELECT @killed;";
        return (int)cmd.ExecuteScalar();
    }

    private static int KillPostgresBackends(string connectionString)
    {
        using var con = new NpgsqlConnection(connectionString);
        con.Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText = @"
SELECT count(*)::int FROM (
    SELECT pg_terminate_backend(pid) AS killed
    FROM pg_stat_activity
    WHERE datname = current_database() AND pid <> pg_backend_pid()
) sub
WHERE killed;";
        return (int)cmd.ExecuteScalar();
    }

    private void SetupSqlEtl(DocumentStore store, string connectionString, string factoryName)
    {
        const string script = @"
loadToUsers({
    Id: id(this),
    Name: this.Name
});";

        var connectionStringName = $"{store.Database}@{store.Urls.First()} to SQL DB";

        Etl.AddEtl(store, new SqlEtlConfiguration
        {
            Name = connectionStringName,
            ConnectionStringName = connectionStringName,
            AllowEtlOnNonEncryptedChannel = true,
            SqlTables =
            {
                new SqlEtlTable { TableName = "Users", DocumentIdColumn = "Id", InsertOnlyMode = false }
            },
            Transforms =
            {
                new Transformation
                {
                    Name = "UsersTransform",
                    Collections = new List<string> { "Users" },
                    Script = script
                }
            }
        }, new SqlConnectionString
        {
            Name = connectionStringName,
            ConnectionString = connectionString,
            FactoryName = factoryName
        });
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
