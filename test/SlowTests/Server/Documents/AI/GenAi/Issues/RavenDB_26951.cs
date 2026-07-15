using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.NotificationCenter.Notifications.Details;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_26951 : RavenTestBase
{
    public RavenDB_26951(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Certificates)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task GenAiTask_InitialContextQuery_ShouldAuthenticateWithFullAccess(bool with2Eku)
    {
        var certificates = Certificates.SetupServerAuthentication(with2Eku: with2Eku);
        var adminCert = Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

        using var store = GetDocumentStore(new Options { AdminCertificate = adminCert, ClientCertificate = adminCert });

        Assert.NotNull(Server.Certificate.ClientCertificate);
        if (with2Eku)
            Assert.Equal(Server.Certificate.ServerCertificate.Thumbprint, Server.Certificate.ClientCertificate.Thumbprint);
        else
            Assert.NotEqual(Server.Certificate.ServerCertificate.Thumbprint, Server.Certificate.ClientCertificate.Thumbprint);

        var deadModelEndpoint = "http://127.0.0.1:1/";

        var config = new GenAiConfiguration
        {
            Name = "genai-initial-query",
            Identifier = "genai-initial-query",
            ConnectionStringName = "genai-cs",
            Collection = "Orders",
            Prompt = "Summarize the orders.",
            SampleObject = "{\"Answer\":\"the answer\"}",
            UpdateScript = "this.Processed = true;",
            Connection = new AiConnectionString
            {
                Name = "genai-cs",
                ModelType = AiModelType.Chat,
                OpenAiSettings = new OpenAiSettings("fake-key", deadModelEndpoint, "gpt-4o")
            },
            GenAiTransformation = new GenAiTransformation
            {
                Script = "ai.genContext({ company: this.Company });"
            },
            Queries =
            [
                new AiAgentToolQuery("RecentOrder", "Get the recent orders", "from Orders limit 5")
                {
                    ParametersSampleObject = "{}",
                    Options = new AiAgentToolQueryOptions { AddToInitialContext = true, AllowModelQueries = false }
                }
            ]
        };

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        store.Maintenance.Send(new AddGenAiOperation(config));

        using (var session = store.OpenSession())
        {
            session.Store(new Order { Company = "companies/1-A", Lines = [new OrderLine { ProductName = "widget", Quantity = 2 }] }, "orders/1-A");
            session.SaveChanges();
        }

        EtlErrorInfo error = null;
        var hadError = await WaitForValueAsync(async () =>
        {
            error = await Etl.TryGetLoadErrorAsync(store.Database, config);
            return error != null;
        }, expectedVal: true, timeout: 60_000);

        Assert.True(hadError, "Expected the GenAI task to record an error (the model endpoint is a dead port).");
        Assert.DoesNotContain("unknown to the server", error.Error, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("InvalidAuth", error.Error, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class Order
    {
        public string Id { get; set; }
        public string Company { get; set; }
        public List<OrderLine> Lines { get; set; }
        public bool Processed { get; set; }
    }
}
