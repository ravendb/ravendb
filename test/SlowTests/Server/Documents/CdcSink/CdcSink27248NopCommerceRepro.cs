using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    [Collection(nameof(CdcSinkPostgresTests))]
    public class CdcSink27248NopCommerceRepro : CdcSinkIntegrationTestBase
    {
        public CdcSink27248NopCommerceRepro(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly string NopConnectionString =
            Environment.GetEnvironmentVariable("RAVEN_NOPCOMMERCE_PG");

        private const string Publication = "cdc_27248_pub";
        private const string Slot = "cdc_27248_slot";
        private const int RangeStart = 40000;
        
        private static TResult WithRetry<TResult>(Func<NpgsqlCommand, TResult> body, string sql)
        {
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    using var conn = new NpgsqlConnection(NopConnectionString);
                    conn.Open();
                    using var cmd = new NpgsqlCommand(sql, conn);
                    cmd.CommandTimeout = 120;
                    return body(cmd);
                }
                catch (Exception e) when (attempt < 90 && IsTransient(e))
                {
                    Thread.Sleep(1000);
                }
            }
        }

        private static bool IsTransient(Exception e) => e switch
        {
            // 57P03 starting up, 57P01 admin shutdown, 08006 connection failure
            PostgresException pe => pe.SqlState is "57P03" or "57P01" or "08006" or "08003",
            NpgsqlException => true,
            _ => false,
        };

        private static void Exec(string sql) => WithRetry(cmd => cmd.ExecuteNonQuery(), sql);

        private static T Scalar<T>(string sql) => WithRetry(cmd =>
        {
            var result = cmd.ExecuteScalar();
            return result is null or DBNull ? default : (T)Convert.ChangeType(result, typeof(T));
        }, sql);

        private static void CleanupReplication()
        {
            for (int attempt = 0; attempt < 15; attempt++)
            {
                try
                {
                    Exec($@"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
                            WHERE slot_name='{Slot}' AND active_pid IS NOT NULL");
                    Exec($"SELECT pg_drop_replication_slot('{Slot}') FROM pg_replication_slots WHERE slot_name='{Slot}'");
                    break;
                }
                catch (PostgresException e) when (e.SqlState == "55006") // object_in_use
                {
                    Thread.Sleep(1000);
                }
            }

            Exec($"DROP PUBLICATION IF EXISTS {Publication}");
        }
        
        private static void WriteBatch(int lo, int hi) => Exec($@"
            INSERT INTO ""GenericAttribute"" (""Id"",""KeyGroup"",""Key"",""Value"",""EntityId"",""StoreId"",""CreatedOrUpdatedDateUTC"")
            SELECT s,'CdcLoad','k'||s,'v'||s,s,1,now() FROM generate_series({lo},{hi}) s
            ON CONFLICT (""Id"") DO NOTHING;");

        private CdcSinkConfiguration BuildConfig(string connectionStringName) => new()
        {
            Name = "nop-27248",
            ConnectionStringName = connectionStringName,
            Postgres = new CdcSinkPostgresSettings
            {
                PublicationName = Publication,
                SlotName = Slot
            },
            Tables = new List<CdcSinkTableConfig>
            {
                new CdcSinkTableConfig
                {
                    CollectionName = "GenericAttributes",
                    SourceTableSchema = "public",
                    SourceTableName = "GenericAttribute",
                    PrimaryKeyColumns = new List<string> { "Id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new CdcColumnMapping { Column = "Id", Name = "SourceId" },
                        new CdcColumnMapping { Column = "EntityId", Name = "EntityId" },
                        new CdcColumnMapping { Column = "KeyGroup", Name = "KeyGroup" },
                        new CdcColumnMapping { Column = "Key", Name = "Key" },
                        new CdcColumnMapping { Column = "Value", Name = "Value" },
                        new CdcColumnMapping { Column = "StoreId", Name = "StoreId" },
                        new CdcColumnMapping { Column = "CreatedOrUpdatedDateUTC", Name = "CreatedOrUpdatedDateUTC" }
                    }
                }
            }
        };

        private class GenericAttr
        {
            public long SourceId { get; set; }
            public long EntityId { get; set; }
            public string KeyGroup { get; set; }
            public string Key { get; set; }
            public string Value { get; set; }
            public string CreatedOrUpdatedDateUTC { get; set; }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task StreamedInserts_KeepTimestamp()
        {
            Assert.False(string.IsNullOrEmpty(NopConnectionString), "RAVEN_NOPCOMMERCE_PG is not set");

            Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            CleanupReplication();

            using var store = GetDocumentStore();
            var sqlCs = new SqlConnectionString
            {
                Name = "nop-pg",
                FactoryName = "Npgsql",
                ConnectionString = NopConnectionString
            };
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));

            var config = BuildConfig(sqlCs.Name);

            try
            {
                AddCdcSink(store, config);
                await WaitForCdcInitialLoadAsync(store, config.Name, timeoutMs: 120_000);
                
                WriteBatch(RangeStart, RangeStart + 249);
                WriteBatch(RangeStart + 250, RangeStart + 499);

                var lastId = RangeStart + 499;
                await AssertWaitForValueAsync(async () =>
                {
                    using var session = store.OpenAsyncSession();
                    var a = await session.LoadAsync<GenericAttr>($"GenericAttributes/{lastId}");
                    return a?.EntityId;
                }, (long)lastId, timeout: 120_000);

                using (var session = store.OpenAsyncSession())
                {
                    foreach (var id in new[] { RangeStart, RangeStart + 123, RangeStart + 250, lastId })
                    {
                        var doc = await session.LoadAsync<GenericAttr>($"GenericAttributes/{id}");
                        Assert.NotNull(doc);
                        var sourceTs = Scalar<DateTime>(
                            $"SELECT \"CreatedOrUpdatedDateUTC\" FROM \"GenericAttribute\" WHERE \"Id\"={id}");

                        Assert.NotNull(doc.CreatedOrUpdatedDateUTC);
                        Assert.Equal(sourceTs.ToString("yyyy-MM-ddTHH:mm:ss.fffffff"), doc.CreatedOrUpdatedDateUTC);
                    }
                }
            }
            finally
            {
                CleanupReplication();
                Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            }
        }
        
        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task ConcurrentWriters_WithSinkAndSourceRestart_KeepTimestamp()
        {
            Assert.False(string.IsNullOrEmpty(NopConnectionString), "RAVEN_NOPCOMMERCE_PG is not set");

            Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            CleanupReplication();

            using var store = GetDocumentStore();
            var sqlCs = new SqlConnectionString
            {
                Name = "nop-pg",
                FactoryName = "Npgsql",
                ConnectionString = NopConnectionString
            };
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));

            var config = BuildConfig(sqlCs.Name);

            try
            {
                AddCdcSink(store, config);
                await WaitForCdcInitialLoadAsync(store, config.Name, timeoutMs: 120_000);
                
                var ranges = new[] { (40000, 49999), (50000, 79999), (80000, 99999) };

                var written = 0;
                var writers = new List<Task>();
                foreach (var (lo, hi) in ranges)
                {
                    var l = lo;
                    var h = hi;
                    writers.Add(Task.Run(() =>
                    {
                        for (int g = l; g <= h; g += 250)
                        {
                            var batchHi = Math.Min(g + 249, h);
                            WriteBatch(g, batchHi);
                            Interlocked.Add(ref written, batchHi - g + 1);
                            Thread.Sleep(300);
                        }
                    }));
                }
                
                var sw = Stopwatch.StartNew();
                await WaitForWrittenAsync(() => written, 9_000, writers);
                Output.WriteLine($"[{sw.Elapsed}] restarting sink at written={written}");
                await RestartSinkAsync(store, config);

                await WaitForWrittenAsync(() => written, 25_000, writers);
                Output.WriteLine($"[{sw.Elapsed}] restarting source at written={written}");
                RestartSourceContainer();
                Output.WriteLine($"[{sw.Elapsed}] source back at written={written}");

                Assert.True(written < 60_000, $"restarts must overlap the writes, but all rows were already written (written={written})");

                await Task.WhenAll(writers);
                Output.WriteLine($"[{sw.Elapsed}] writers done");

                var srcRange = Scalar<long>($@"SELECT count(*) FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
                var srcTotal = Scalar<long>(@"SELECT count(*) FROM ""GenericAttribute""");
                Assert.Equal(60_000, srcRange);
                
                var ravenCount = await WaitForValueAsync(async () =>
                {
                    using var session = store.OpenAsyncSession();
                    return await session.Query<GenericAttr>(collectionName: "GenericAttributes").CountAsync();
                }, (int)srcTotal, timeout: 300_000, interval: 2000);

                Output.WriteLine($"source total={srcTotal} range={srcRange} raven={ravenCount}");
                
                var srcTimestamps = new Dictionary<long, DateTime?>();
                WithRetry(cmd =>
                {
                    using var reader = cmd.ExecuteReader();
                    while (reader.Read())
                        srcTimestamps[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
                    return 0;
                }, $@"SELECT ""Id"", ""CreatedOrUpdatedDateUTC"" FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");

                var nulls = new List<long>();
                var mismatches = new List<string>();
                var missing = new List<long>();

                foreach (var chunk in srcTimestamps.Keys.OrderBy(k => k).Chunk(1024))
                {
                    using var session = store.OpenAsyncSession();
                    var ids = chunk.Select(id => $"GenericAttributes/{id}").ToArray();
                    var docs = await session.LoadAsync<GenericAttr>(ids);

                    foreach (var id in chunk)
                    {
                        if (docs.TryGetValue($"GenericAttributes/{id}", out var doc) == false || doc is null)
                        {
                            missing.Add(id);
                            continue;
                        }

                        if (doc.CreatedOrUpdatedDateUTC is null)
                        {
                            nulls.Add(id);
                            continue;
                        }

                        var expected = srcTimestamps[id]?.ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
                        if (expected != doc.CreatedOrUpdatedDateUTC && mismatches.Count < 20)
                            mismatches.Add($"{id}: source={expected} raven={doc.CreatedOrUpdatedDateUTC}");
                    }
                }

                Output.WriteLine($"missing={missing.Count} nullTimestamps={nulls.Count} mismatches={mismatches.Count}");
                if (nulls.Count > 0)
                    Output.WriteLine("first null ids: " + string.Join(", ", nulls.Take(20)));
                if (missing.Count > 0)
                    Output.WriteLine("first missing ids: " + string.Join(", ", missing.Take(20)));
                foreach (var m in mismatches)
                    Output.WriteLine(m);

                Assert.Empty(nulls);
                Assert.Empty(mismatches);
                Assert.Empty(missing);
            }
            finally
            {
                CleanupReplication();
                Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            }
        }
        
        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task UnmappedTimestampColumn_LooksIdenticalToANullValue()
        {
            Assert.False(string.IsNullOrEmpty(NopConnectionString), "RAVEN_NOPCOMMERCE_PG is not set");

            Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            CleanupReplication();

            using var store = GetDocumentStore();
            var sqlCs = new SqlConnectionString
            {
                Name = "nop-pg",
                FactoryName = "Npgsql",
                ConnectionString = NopConnectionString
            };
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));

            var config = BuildConfig(sqlCs.Name);
            config.Tables[0].Columns.RemoveAll(c => c.Column == "CreatedOrUpdatedDateUTC");

            try
            {
                AddCdcSink(store, config);
                await WaitForCdcInitialLoadAsync(store, config.Name, timeoutMs: 120_000);

                WriteBatch(RangeStart, RangeStart + 9);

                await AssertWaitForValueAsync(async () =>
                {
                    using var session = store.OpenAsyncSession();
                    var a = await session.LoadAsync<GenericAttr>($"GenericAttributes/{RangeStart}");
                    return a?.EntityId;
                }, (long)RangeStart, timeout: 120_000);
                
                var sourceTs = Scalar<DateTime>(
                    $"SELECT \"CreatedOrUpdatedDateUTC\" FROM \"GenericAttribute\" WHERE \"Id\"={RangeStart}");
                Assert.NotEqual(default, sourceTs);
                
                using (var http = new System.Net.Http.HttpClient())
                {
                    var json = await http.GetStringAsync(
                        $"{store.Urls[0]}/databases/{store.Database}/docs?id=GenericAttributes/{RangeStart}");
                    Output.WriteLine("document JSON: " + json);
                    Assert.DoesNotContain("CreatedOrUpdatedDateUTC", json);
                }
            }
            finally
            {
                CleanupReplication();
                Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            }
        }

        private static async Task WaitForWrittenAsync(Func<int> written, int target, List<Task> writers)
        {
            while (written() < target)
            {
                if (writers.All(w => w.IsCompleted))
                    return;
                await Task.Delay(100);
            }
        }

        private async Task RestartSinkAsync(IDocumentStore store, CdcSinkConfiguration config)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.First(p => p.Name == config.Name);
            process.Stop("RavenDB-27248 repro restart");
            await Task.Delay(TimeSpan.FromSeconds(2));
            process.Start();
        }

        private static void RestartSourceContainer()
        {
            var psi = new System.Diagnostics.ProcessStartInfo("docker", "restart nop-postgres")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
            using var p = System.Diagnostics.Process.Start(psi);
            p.WaitForExit(120_000);
        }
    }
}
