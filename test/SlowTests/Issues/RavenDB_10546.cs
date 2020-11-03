using System;
using System.Net.Http;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10546 : RavenTestBase
    {
        public RavenDB_10546(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSetStudioConfiguration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                ServerWideStudioConfiguration serverWideStudioConfiguration = store.Maintenance.Server.Send(new GetServerWideStudioConfigurationOperation());

                Assert.Null(serverWideStudioConfiguration);

                StudioConfiguration studioConfiguration = store.Maintenance.Send(new GetStudioConfigurationOperation());

                Assert.Null(studioConfiguration);

                store.Maintenance.Server.Send(new PutServerWideStudioConfigurationOperation(new ServerWideStudioConfiguration
                {
                    Environment = StudioConfiguration.StudioEnvironment.Development,
                    ReplicationFactor = 2
                }));

                serverWideStudioConfiguration = store.Maintenance.Server.Send(new GetServerWideStudioConfigurationOperation());

                Assert.NotNull(serverWideStudioConfiguration);
                Assert.Equal(StudioConfiguration.StudioEnvironment.Development, serverWideStudioConfiguration.Environment); // from server
                Assert.Equal(2, serverWideStudioConfiguration.ReplicationFactor);

                studioConfiguration = store.Maintenance.Send(new GetStudioConfigurationOperation());

                Assert.Null(studioConfiguration);

                store.Maintenance.Send(new PutStudioConfigurationOperation(new StudioConfiguration
                {
                    Environment = StudioConfiguration.StudioEnvironment.Production
                }));

                studioConfiguration = store.Maintenance.Send(new GetStudioConfigurationOperation());

                Assert.NotNull(studioConfiguration);
                Assert.Equal(StudioConfiguration.StudioEnvironment.Production, studioConfiguration.Environment); // from database

                store.Maintenance.Server.Send(new PutServerWideStudioConfigurationOperation(new ServerWideStudioConfiguration
                {
                    Environment = StudioConfiguration.StudioEnvironment.None
                }));

                studioConfiguration = store.Maintenance.Send(new GetStudioConfigurationOperation());

                Assert.NotNull(studioConfiguration);
                Assert.Equal(StudioConfiguration.StudioEnvironment.Production, studioConfiguration.Environment); // from database

                store.Maintenance.Send(new PutStudioConfigurationOperation(new StudioConfiguration
                {
                    Environment = StudioConfiguration.StudioEnvironment.Production,
                    Disabled = true
                }));

                studioConfiguration = store.Maintenance.Send(new GetStudioConfigurationOperation());

                Assert.NotNull(studioConfiguration);
                Assert.True(studioConfiguration.Disabled);
                Assert.Equal(StudioConfiguration.StudioEnvironment.Production, studioConfiguration.Environment); // from database
            }
        }

        internal class GetStudioConfigurationOperation : IMaintenanceOperation<StudioConfiguration>
        {
            public RavenCommand<StudioConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetStudioConfigurationCommand();
            }

            private class GetStudioConfigurationCommand : RavenCommand<StudioConfiguration>
            {
                public override bool IsReadRequest => false;
                

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/configuration/studio";

                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Get
                    };

                    return request;
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        return;

                    Result = JsonDeserializationClient.StudioConfiguration(response);
                }
            }
        }

        internal class PutStudioConfigurationOperation : IMaintenanceOperation
        {
            private readonly StudioConfiguration _configuration;

            public PutStudioConfigurationOperation(StudioConfiguration configuration)
            {
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new PutStudioConfigurationCommand(conventions, context, _configuration);
            }

            private class PutStudioConfigurationCommand : RavenCommand, IRaftCommand
            {
                private readonly BlittableJsonReaderObject _configuration;

                public PutStudioConfigurationCommand(DocumentConventions conventions, JsonOperationContext context, StudioConfiguration configuration)
                {
                    if (conventions == null)
                        throw new ArgumentNullException(nameof(conventions));
                    if (configuration == null)
                        throw new ArgumentNullException(nameof(configuration));
                    if (context == null)
                        throw new ArgumentNullException(nameof(context));

                    _configuration = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context);
                }


                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/admin/configuration/studio";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Put,
                        Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _configuration).ConfigureAwait(false))
                    };
                }

                public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
            }
        }

        internal class PutServerWideStudioConfigurationOperation : IServerOperation
        {
            private readonly ServerWideStudioConfiguration _configuration;

            public PutServerWideStudioConfigurationOperation(ServerWideStudioConfiguration configuration)
            {
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new PutServerWideStudioConfigurationCommand(conventions, context, _configuration);
            }

            private class PutServerWideStudioConfigurationCommand : RavenCommand, IRaftCommand
            {
                private readonly BlittableJsonReaderObject _configuration;

                public PutServerWideStudioConfigurationCommand(DocumentConventions conventions, JsonOperationContext context, ServerWideStudioConfiguration configuration)
                {
                    if (conventions == null)
                        throw new ArgumentNullException(nameof(conventions));
                    if (configuration == null)
                        throw new ArgumentNullException(nameof(configuration));
                    if (context == null)
                        throw new ArgumentNullException(nameof(context));

                    _configuration = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context);
                }


                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/admin/configuration/studio";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Put,
                        Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _configuration).ConfigureAwait(false))
                    };
                }

                public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
            }
        }

        internal class GetServerWideStudioConfigurationOperation : IServerOperation<ServerWideStudioConfiguration>
        {
            public RavenCommand<ServerWideStudioConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetServerWideStudioConfigurationCommand();
            }

            private class GetServerWideStudioConfigurationCommand : RavenCommand<ServerWideStudioConfiguration>
            {
                public override bool IsReadRequest => false;
                

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/configuration/studio";

                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Get
                    };

                    return request;
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        return;

                    Result = JsonDeserializationClient.ServerWideStudioConfiguration(response);
                }
            }
        }
    }
}
