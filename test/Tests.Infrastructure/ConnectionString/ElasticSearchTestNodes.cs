using System;
using Nest;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;

namespace Tests.Infrastructure.ConnectionString
{
    public class ElasticSearchTestNodes
    {
        private const string EnvironmentVariable = "RAVEN_ELASTICSEARCH_NODE_URLS";

        private static ElasticSearchTestNodes _instance;

        public static ElasticSearchTestNodes Instance => _instance ??= new ElasticSearchTestNodes();

        private ElasticSearchTestNodes()
        {
            VerifiedNodes = new Lazy<string[]>(VerifiedNodesValueFactory);

            Nodes = new Lazy<string[]>(() =>
            {
                var nodes = Environment.GetEnvironmentVariable(EnvironmentVariable);

                return string.IsNullOrEmpty(nodes)
                    ? Array.Empty<string>()
                    : nodes.Split(new[] { ',', ';' }, StringSplitOptions.TrimEntries);
            });
        }

        private Lazy<string[]> Nodes { get; }

        public Lazy<string[]> VerifiedNodes { get; }

        public bool CanConnect()
        {
            try
            {
                VerifiedNodesValueFactory();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        protected virtual string[] VerifiedNodesValueFactory()
        {
            var singleLocalNode = new[] { "http://localhost:9200" };

            if (TryConnect(singleLocalNode, out var pingResponse))
                return singleLocalNode;

            if (Nodes.Value.Length == 0)
                throw new InvalidOperationException($"Environment variable {EnvironmentVariable} is empty");


            if (TryConnect(Nodes.Value, out pingResponse))
                return Nodes.Value;

            throw new InvalidOperationException($"Can't ping Elastic Search instance. Provided urls: {string.Join(',', Nodes.Value)}", pingResponse?.OriginalException);


            bool TryConnect(string[] nodes, out PingResponse response)
            {
                try
                {
                    var client = ElasticSearchHelper.CreateClient(new ElasticSearchConnectionString { Nodes = nodes }, requestTimeout: TimeSpan.FromSeconds(1), pingTimeout: TimeSpan.FromSeconds(1));

                    response = client.Ping();

                    return response.IsValid;
                }
                catch
                {
                    response = null;

                    return false;
                }
            }
        }
    }
}
