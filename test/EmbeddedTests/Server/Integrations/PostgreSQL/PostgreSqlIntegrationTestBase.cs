#if NET8_0
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Embedded;
using Sparrow.Json;

namespace EmbeddedTests.Server.Integrations.PostgreSQL
{
    public class PostgreSqlIntegrationTestBase : EmbeddedTestBase
    {
        private static readonly object PortLocker = new object();
        private static List<int> _takenPorts = _takenPorts = new List<int>();

        private EmbeddedServer _embeddedServer;
        private int _port;

        protected const string CorrectUser = "root";
        protected const string CorrectPassword = "s3cr3t";

        protected IDocumentStore GetDocumentStore([CallerMemberName] string caller = null, bool sharded = false)
        {
            var options = CopyServerAndCreateOptions();

            _embeddedServer = new EmbeddedServer();

            _port = GetAvailablePort(10000);

            options.CommandLineArgs = [
                "--Integrations.PostgreSQL.Enabled=true",
                $"Integrations.PostgreSQL.Port={_port}" // a free port will be allocated so tests can run in parallel
            ];

            _embeddedServer.StartServer(options);

            if (sharded)
            {
                return _embeddedServer.GetDocumentStore(new DatabaseOptions(new DatabaseRecord(caller)
                {
                    Sharding = new ShardingConfiguration()
                    {
                        Shards = new Dictionary<int, DatabaseTopology>()
                        {
                            {0, new DatabaseTopology()},
                            {1, new DatabaseTopology()},
                            {2, new DatabaseTopology()},
                        }
                    }
                }));
            }

            return _embeddedServer.GetDocumentStore(caller);
        }

        public static int GetAvailablePort(int startingPort)
        {
            if (startingPort > ushort.MaxValue)
                throw new ArgumentException($"Can't be greater than {ushort.MaxValue}", nameof(startingPort));

            lock (PortLocker)
            {
                var ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();

                var connectionsEndpoints = ipGlobalProperties.GetActiveTcpConnections().Select(c => c.LocalEndPoint);
                var tcpListenersEndpoints = ipGlobalProperties.GetActiveTcpListeners();
                var udpListenersEndpoints = ipGlobalProperties.GetActiveUdpListeners();
                var portsInUse = connectionsEndpoints.Concat(tcpListenersEndpoints)
                    .Concat(udpListenersEndpoints)
                    .Select(e => e.Port)
                    .Concat(_takenPorts);

                int result = Enumerable.Range(startingPort, ushort.MaxValue - startingPort + 1).Except(portsInUse).FirstOrDefault();

                _takenPorts.Add(result);

                return result;
            }
        }

        private DataTable ExecuteSqlQuery( NpgsqlConnection connection, string query, Dictionary<string, (NpgsqlDbType, object)> namedArgs = null, bool prepareExecute = false)
        {
            using var cmd = new NpgsqlCommand(query, connection);

            if (namedArgs != null)
            {
                foreach (var (key, val) in namedArgs)
                {
                    cmd.Parameters.AddWithValue(key, val.Item1, val.Item2);
                }
            }

            if (prepareExecute)
                cmd.Prepare();
               
            using var reader = cmd.ExecuteReader();
            var dt = new DataTable();
            dt.Load(reader);

            return dt;
        }

        protected async Task<DataTable> Act(IDocumentStore store, string query, bool? forceSslMode = null,
            Dictionary<string, (NpgsqlDbType, object)> parameters = null, bool prepareExecute = false)
        {
            var connectionString = GetConnectionString(store, forceSslMode);

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var result = ExecuteSqlQuery(connection, query, parameters, prepareExecute);

            await connection.CloseAsync();
            return result;
        }

        protected string GetConnectionString(IDocumentStore store, bool? forceSslMode = null)
        {
            var uri = new Uri(store.Urls.First());

            var host = IPAddress.Parse(uri.Host).ToString();

            var database = store.Database;

            string connectionString;

            //if (server.Certificate.Certificate == null || forceSslMode == false)
                connectionString = $"Host={host};Port={_port};Database={database};Uid={CorrectUser};Include Error Detail=True";
            //else
            //    connectionString = $"Host={host};Port={_port};Database={database};Uid={CorrectUser};Password={CorrectPassword};SSL Mode=Prefer;Trust Server Certificate=true";

            return connectionString;
        }

        protected void WaitForIndexing(IDocumentStore store, string database = null, TimeSpan? timeout = null)
        {
            var admin = store.Maintenance.ForDatabase(database);

            timeout = timeout ?? TimeSpan.FromMinutes(1);

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var databaseStatistics = admin.Send(new GetStatisticsOperation());
                var indexes = databaseStatistics.Indexes
                    .Where(x => x.State != IndexState.Disabled);

                if (indexes.All(x => x.IsStale == false
                                     && x.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix) == false))
                    return;

                if (databaseStatistics.Indexes.Any(x => x.State == IndexState.Error))
                {
                    break;
                }

                Thread.Sleep(100);
            }

            var errors = admin.Send(new GetIndexErrorsOperation());

            string allIndexErrorsText = string.Empty;
            if (errors != null && errors.Length > 0)
            {
                var allIndexErrorsListText = string.Join("\r\n",
                    errors.Select(FormatIndexErrors));
                allIndexErrorsText = $"Indexing errors:\r\n{allIndexErrorsListText}";

                string FormatIndexErrors(IndexErrors indexErrors)
                {
                    var errorsListText = string.Join("\r\n",
                        indexErrors.Errors.Select(x => $"- {x}"));
                    return $"Index '{indexErrors.Name}' ({indexErrors.Errors.Length} errors):\r\n{errorsListText}";
                }
            }

            throw new TimeoutException($"The indexes stayed stale for more than {timeout.Value}.{allIndexErrorsText}");
        }


        protected class CreateSampleDataOperation : IMaintenanceOperation
        {
            private readonly DatabaseItemType _operateOnTypes;

            public CreateSampleDataOperation(DatabaseItemType operateOnTypes = DatabaseItemType.Documents)
            {
                _operateOnTypes = operateOnTypes;
            }

            public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new CreateSampleDataCommand(_operateOnTypes);
            }

            private class CreateSampleDataCommand : RavenCommand, IRaftCommand
            {
                private readonly DatabaseItemType _operateOnTypes;

                public CreateSampleDataCommand(DatabaseItemType operateOnTypes)
                {
                    _operateOnTypes = operateOnTypes;
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/studio/sample-data");

                    var operateOnTypes = _operateOnTypes.ToString().Split(",", StringSplitOptions.RemoveEmptyEntries);
                    for (var i = 0; i < operateOnTypes.Length; i++)
                    {
                        sb.Append(i == 0 ? "?" : "&");
                        sb.Append("operateOnTypes=");
                        sb.Append(operateOnTypes[i].Trim());
                    }

                    url = sb.ToString();

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Post
                    };
                }

                public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
            }
        }

        public override void Dispose()
        {
            _embeddedServer.Dispose();

            base.Dispose();
        }
        protected class Company
        {
            public string Id { get; set; }
            public string ExternalId { get; set; }
            public string Name { get; set; }
            public Contact Contact { get; set; }
            public Address Address { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
        }

        protected class Address
        {
            public string Line1 { get; set; }
            public string Line2 { get; set; }
            public string City { get; set; }
            public string Region { get; set; }
            public string PostalCode { get; set; }
            public string Country { get; set; }
        }

        protected class Contact
        {
            public string Name { get; set; }
            public string Title { get; set; }
        }

        protected class Category
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
        }

        protected class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public string Employee { get; set; }
            public DateTime OrderedAt { get; set; }
            public DateTime RequireAt { get; set; }
            public DateTime? ShippedAt { get; set; }
            public Address ShipTo { get; set; }
            public string ShipVia { get; set; }
            public decimal Freight { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        protected class OrderLine
        {
            public string Product { get; set; }
            public string ProductName { get; set; }
            public decimal PricePerUnit { get; set; }
            public int Quantity { get; set; }
            public decimal Discount { get; set; }
        }

        protected class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Supplier { get; set; }
            public string Category { get; set; }
            public string QuantityPerUnit { get; set; }
            public decimal PricePerUnit { get; set; }
            public int UnitsInStock { get; set; }
            public int UnitsOnOrder { get; set; }
            public bool Discontinued { get; set; }
            public int ReorderLevel { get; set; }
        }

        protected class Supplier
        {
            public string Id { get; set; }
            public Contact Contact { get; set; }
            public string Name { get; set; }
            public Address Address { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
            public string HomePage { get; set; }
        }

        protected class Employee
        {
            public string Id { get; set; }
            public string LastName { get; set; }
            public string FirstName { get; set; }
            public string Title { get; set; }
            public Address Address { get; set; }
            public DateTime HiredAt { get; set; }
            public DateTime Birthday { get; set; }
            public string HomePhone { get; set; }
            public string Extension { get; set; }
            public string ReportsTo { get; set; }
            public List<string> Notes { get; set; }
            public List<string> Territories { get; set; }
        }

        protected class Region
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public List<Territory> Territories { get; set; }
        }

        protected class Territory
        {
            public string Code { get; set; }
            public string Name { get; set; }
        }

        protected class Shipper
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Phone { get; set; }
        }
    }
}
#endif
