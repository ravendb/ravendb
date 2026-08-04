using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Server.Config;
using Raven.Server.Monitoring.Snmp;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27081 : RavenTestBase
{
    public RavenDB_27081(ITestOutputHelper output) : base(output)
    {
    }

    private static IEnumerable<(string Name, string Oid)> GeneralOids() =>
        typeof(SnmpOids.Databases.General)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(x => x.FieldType == typeof(string))
            .Select(x => (x.Name, (string)x.GetRawConstantValue()));

    private void UseServerWithSnmp()
    {
        UseNewLocalServer(new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Enabled)] = "true",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Port)] = GetBindableSnmpPort().ToString()
        });
    }

    private static int GetBindableSnmpPort()
    {
        using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        probe.Bind(new IPEndPoint(IPAddress.Any, 0));
        return ((IPEndPoint)probe.LocalEndPoint).Port;
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public void EveryDeclaredGeneralOidIsRegistered()
    {
        UseServerWithSnmp();

        var unregistered = GeneralOids()
            .Where(x => Server.SnmpWatcher.GetData(SnmpOids.Root + x.Oid) == null)
            .Select(x => $"{x.Name} ({SnmpOids.Root + x.Oid})")
            .ToList();

        Assert.True(unregistered.Count == 0,
            $"These OIDs are declared in {nameof(SnmpOids)} but were never added to the SNMP object store, " +
            $"so /monitoring/snmp returns 404 for them: {string.Join(", ", unregistered)}");
    }

    [RavenTheory(RavenTestCategory.Monitoring)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfQueueSinkTasks)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfActiveQueueSinkTasks)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfSnowflakeEtlTasks)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfActiveSnowflakeEtlTasks)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfEmbeddingGenerationTasks)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfActiveEmbeddingGenerationTasks)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfGenAiTasks)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfActiveGenAiTasks)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfCdcSinkTasks)]
    [InlineData(SnmpOids.Databases.General.TotalNumberOfActiveCdcSinkTasks)]
    public void OngoingTaskCountOidsReportZeroInsteadOf404(string oid)
    {
        UseServerWithSnmp();

        var data = Server.SnmpWatcher.GetData(SnmpOids.Root + oid);

        Assert.NotNull(data);
        Assert.Equal(0, Assert.IsType<Integer32>(data).ToInt32());
    }

    [RavenFact(RavenTestCategory.Monitoring | RavenTestCategory.Etl)]
    public async Task QueueSinkTaskIsCountedByItsOwnOidAndByTheAggregate()
    {
        UseServerWithSnmp();

        using (var store = GetDocumentStore())
        {
            var ongoingBefore = GetInt(SnmpOids.Databases.General.TotalNumberOfOngoingTasks);
            var activeBefore = GetInt(SnmpOids.Databases.General.TotalNumberOfActiveOngoingTasks);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<QueueConnectionString>(new QueueConnectionString
            {
                Name = "kafka-cs",
                BrokerType = QueueBrokerType.Kafka,
                KafkaConnectionSettings = new KafkaConnectionSettings { BootstrapServers = "localhost:9092" }
            }));

            await store.Maintenance.SendAsync(new AddQueueSinkOperation<QueueConnectionString>(new QueueSinkConfiguration
            {
                Name = "sink-1",
                ConnectionStringName = "kafka-cs",
                BrokerType = QueueBrokerType.Kafka,
                Scripts =
                [
                    new QueueSinkScript { Name = "script-1", Queues = ["users"], Script = "put(this.Id, this)" }
                ]
            }));

            Assert.Equal(1, WaitForValue(() => GetInt(SnmpOids.Databases.General.TotalNumberOfQueueSinkTasks), 1, interval: 500));
            Assert.Equal(ongoingBefore + 1, WaitForValue(() => GetInt(SnmpOids.Databases.General.TotalNumberOfOngoingTasks), ongoingBefore + 1, interval: 500));

            Assert.Equal(1, WaitForValue(() => GetInt(SnmpOids.Databases.General.TotalNumberOfActiveQueueSinkTasks), 1, interval: 500));
            Assert.Equal(activeBefore + 1, WaitForValue(() => GetInt(SnmpOids.Databases.General.TotalNumberOfActiveOngoingTasks), activeBefore + 1, interval: 500));
        }

        int GetInt(string oid) => ((Integer32)Server.SnmpWatcher.GetData(SnmpOids.Root + oid)).ToInt32();
    }
}
