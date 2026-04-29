using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    /// <summary>
    /// Tests a SQL connection string at the cluster level (no database context required).
    /// </summary>
    public sealed class TestSqlConnectionStringOperation : IServerOperation<ConnectionStringTestResult>
    {
        private readonly string _factoryName;
        private readonly string _connectionString;

        /// <inheritdoc cref="TestSqlConnectionStringOperation"/>
        /// <param name="factoryName">The ADO.NET factory name (e.g. <c>System.Data.SqlClient</c>).</param>
        /// <param name="connectionString">The SQL connection string to validate.</param>
        public TestSqlConnectionStringOperation(string factoryName, string connectionString)
        {
            _factoryName = factoryName ?? throw new ArgumentNullException(nameof(factoryName));
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        public RavenCommand<ConnectionStringTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            => new TestSqlConnectionStringCommand(_factoryName, _connectionString);

        private sealed class TestSqlConnectionStringCommand : TestConnectionStringCommandBase
        {
            private readonly string _factoryName;
            private readonly string _connectionString;

            public TestSqlConnectionStringCommand(string factoryName, string connectionString)
            {
                _factoryName = factoryName;
                _connectionString = connectionString;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/sql/test-connection?factoryName={Uri.EscapeDataString(_factoryName)}";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new StringContent(_connectionString, Encoding.UTF8)
                };
            }
        }
    }

    /// <summary>
    /// Tests a Snowflake connection string at the cluster level.
    /// </summary>
    public sealed class TestSnowflakeConnectionStringOperation : IServerOperation<ConnectionStringTestResult>
    {
        private readonly string _connectionString;

        /// <inheritdoc cref="TestSnowflakeConnectionStringOperation"/>
        /// <param name="connectionString">The Snowflake connection string to validate.</param>
        public TestSnowflakeConnectionStringOperation(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        public RavenCommand<ConnectionStringTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            => new TestSnowflakeConnectionStringCommand(_connectionString);

        private sealed class TestSnowflakeConnectionStringCommand : TestConnectionStringCommandBase
        {
            private readonly string _connectionString;

            public TestSnowflakeConnectionStringCommand(string connectionString)
            {
                _connectionString = connectionString;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/snowflake/test-connection";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new StringContent(_connectionString, Encoding.UTF8)
                };
            }
        }
    }

    /// <summary>
    /// Tests an ElasticSearch connection at the cluster level.
    /// </summary>
    public sealed class TestElasticSearchConnectionStringOperation : IServerOperation<ConnectionStringTestResult>
    {
        private readonly string _serverUrl;
        private readonly Authentication _authentication;

        /// <inheritdoc cref="TestElasticSearchConnectionStringOperation"/>
        /// <param name="serverUrl">The ElasticSearch node URL.</param>
        /// <param name="authentication">Authentication settings (may be <c>null</c>).</param>
        public TestElasticSearchConnectionStringOperation(string serverUrl, Authentication authentication)
        {
            _serverUrl = serverUrl ?? throw new ArgumentNullException(nameof(serverUrl));
            _authentication = authentication;
        }

        public RavenCommand<ConnectionStringTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            => new TestElasticSearchConnectionStringCommand(conventions, _serverUrl, _authentication);

        private sealed class TestElasticSearchConnectionStringCommand : TestConnectionStringCommandBase
        {
            private readonly DocumentConventions _conventions;
            private readonly string _serverUrl;
            private readonly Authentication _authentication;

            public TestElasticSearchConnectionStringCommand(DocumentConventions conventions, string serverUrl, Authentication authentication)
            {
                _conventions = conventions;
                _serverUrl = serverUrl;
                _authentication = authentication;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/elasticsearch/test-connection?url={Uri.EscapeDataString(_serverUrl)}";

                var json = _conventions.Serialization.DefaultConverter.ToBlittable(_authentication ?? new Authentication(), ctx);
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, json).ConfigureAwait(false), _conventions)
                };
            }
        }
    }

    /// <summary>
    /// Tests a Kafka connection at the cluster level.
    /// </summary>
    public sealed class TestKafkaConnectionStringOperation : IServerOperation<ConnectionStringTestResult>
    {
        private readonly KafkaConnectionSettings _settings;

        /// <inheritdoc cref="TestKafkaConnectionStringOperation"/>
        /// <param name="settings">The Kafka connection settings to validate.</param>
        public TestKafkaConnectionStringOperation(KafkaConnectionSettings settings)
        {
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        }

        public RavenCommand<ConnectionStringTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            => new TestKafkaConnectionStringCommand(conventions, _settings);

        private sealed class TestKafkaConnectionStringCommand : TestConnectionStringCommandBase
        {
            private readonly DocumentConventions _conventions;
            private readonly KafkaConnectionSettings _settings;

            public TestKafkaConnectionStringCommand(DocumentConventions conventions, KafkaConnectionSettings settings)
            {
                _conventions = conventions;
                _settings = settings;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/queue/kafka/test-connection";
                var json = _conventions.Serialization.DefaultConverter.ToBlittable(_settings, ctx);
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, json).ConfigureAwait(false), _conventions)
                };
            }
        }
    }

    /// <summary>
    /// Tests a RabbitMQ connection at the cluster level.
    /// </summary>
    public sealed class TestRabbitMqConnectionStringOperation : IServerOperation<ConnectionStringTestResult>
    {
        private readonly RabbitMqConnectionSettings _settings;

        /// <inheritdoc cref="TestRabbitMqConnectionStringOperation"/>
        /// <param name="settings">The RabbitMQ connection settings to validate.</param>
        public TestRabbitMqConnectionStringOperation(RabbitMqConnectionSettings settings)
        {
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        }

        public RavenCommand<ConnectionStringTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            => new TestRabbitMqConnectionStringCommand(conventions, _settings);

        private sealed class TestRabbitMqConnectionStringCommand : TestConnectionStringCommandBase
        {
            private readonly DocumentConventions _conventions;
            private readonly RabbitMqConnectionSettings _settings;

            public TestRabbitMqConnectionStringCommand(DocumentConventions conventions, RabbitMqConnectionSettings settings)
            {
                _conventions = conventions;
                _settings = settings;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/queue/rabbitmq/test-connection";
                var json = _conventions.Serialization.DefaultConverter.ToBlittable(_settings, ctx);
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, json).ConfigureAwait(false), _conventions)
                };
            }
        }
    }

    /// <summary>
    /// Tests an Azure Queue Storage connection at the cluster level.
    /// </summary>
    public sealed class TestAzureQueueStorageConnectionStringOperation : IServerOperation<ConnectionStringTestResult>
    {
        private readonly AzureQueueStorageConnectionSettings _settings;

        /// <inheritdoc cref="TestAzureQueueStorageConnectionStringOperation"/>
        /// <param name="settings">The Azure Queue Storage connection settings to validate.</param>
        public TestAzureQueueStorageConnectionStringOperation(AzureQueueStorageConnectionSettings settings)
        {
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        }

        public RavenCommand<ConnectionStringTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            => new TestAzureQueueStorageConnectionStringCommand(conventions, _settings);

        private sealed class TestAzureQueueStorageConnectionStringCommand : TestConnectionStringCommandBase
        {
            private readonly DocumentConventions _conventions;
            private readonly AzureQueueStorageConnectionSettings _settings;

            public TestAzureQueueStorageConnectionStringCommand(DocumentConventions conventions, AzureQueueStorageConnectionSettings settings)
            {
                _conventions = conventions;
                _settings = settings;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/queue/azurequeuestorage/test-connection";
                var json = _conventions.Serialization.DefaultConverter.ToBlittable(_settings, ctx);
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, json).ConfigureAwait(false), _conventions)
                };
            }
        }
    }

    /// <summary>
    /// Tests an Amazon SQS connection at the cluster level.
    /// </summary>
    public sealed class TestAmazonSqsConnectionStringOperation : IServerOperation<ConnectionStringTestResult>
    {
        private readonly AmazonSqsConnectionSettings _settings;

        /// <inheritdoc cref="TestAmazonSqsConnectionStringOperation"/>
        /// <param name="settings">The Amazon SQS connection settings to validate.</param>
        public TestAmazonSqsConnectionStringOperation(AmazonSqsConnectionSettings settings)
        {
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        }

        public RavenCommand<ConnectionStringTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            => new TestAmazonSqsConnectionStringCommand(conventions, _settings);

        private sealed class TestAmazonSqsConnectionStringCommand : TestConnectionStringCommandBase
        {
            private readonly DocumentConventions _conventions;
            private readonly AmazonSqsConnectionSettings _settings;

            public TestAmazonSqsConnectionStringCommand(DocumentConventions conventions, AmazonSqsConnectionSettings settings)
            {
                _conventions = conventions;
                _settings = settings;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/queue/amazonsqs/test-connection";
                var json = _conventions.Serialization.DefaultConverter.ToBlittable(_settings, ctx);
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, json).ConfigureAwait(false), _conventions)
                };
            }
        }
    }

    /// <summary>
    /// Tests an AI connection at the cluster level.
    /// </summary>
    public sealed class TestAiConnectionStringOperation : IServerOperation<ConnectionStringTestResult>
    {
        private readonly AiConnectorType _connectorType;
        private readonly AiModelType _modelType;
        private readonly object _settings;

        /// <inheritdoc cref="TestAiConnectionStringOperation"/>
        /// <param name="connectorType">The AI connector type (e.g. <c>OpenAi</c>, <c>AzureOpenAi</c>, <c>Ollama</c>).</param>
        /// <param name="modelType">The AI model type (e.g. <c>TextEmbeddings</c>, <c>Chat</c>).</param>
        /// <param name="settings">The connector-specific settings object (e.g. <see cref="OpenAiSettings"/>, <see cref="OllamaSettings"/>).</param>
        public TestAiConnectionStringOperation(AiConnectorType connectorType, AiModelType modelType, object settings)
        {
            if (connectorType == AiConnectorType.None)
                throw new ArgumentException($"AI connector type cannot be '{AiConnectorType.None}'", nameof(connectorType));
            _connectorType = connectorType;
            _modelType = modelType;
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        }

        public RavenCommand<ConnectionStringTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            => new TestAiConnectionStringCommand(conventions, _connectorType, _modelType, _settings);

        private sealed class TestAiConnectionStringCommand : TestConnectionStringCommandBase
        {
            private readonly DocumentConventions _conventions;
            private readonly AiConnectorType _connectorType;
            private readonly AiModelType _modelType;
            private readonly object _settings;

            public TestAiConnectionStringCommand(DocumentConventions conventions, AiConnectorType connectorType, AiModelType modelType, object settings)
            {
                _conventions = conventions;
                _connectorType = connectorType;
                _modelType = modelType;
                _settings = settings;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/ai/test-connection?type={_connectorType}&modelType={_modelType}";
                var json = _conventions.Serialization.DefaultConverter.ToBlittable(_settings, ctx);
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, json).ConfigureAwait(false), _conventions)
                };
            }
        }
    }

    internal abstract class TestConnectionStringCommandBase : RavenCommand<ConnectionStringTestResult>
    {
        public override bool IsReadRequest => false;

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.ConnectionStringTestResult(response);
        }
    }
}
