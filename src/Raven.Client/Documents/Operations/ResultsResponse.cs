using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Transformers;
using Raven.Client.Server.Operations.Certificates;

namespace Raven.Client.Documents.Operations
{
    internal abstract class ResultsResponse<T>
    {
        public T[] Results { get; set; }
    }

    internal class GetIndexNamesResponse : ResultsResponse<string>
    {
    }

    internal class GetTransformerNamesResponse : ResultsResponse<string>
    {
    }

    internal class PutIndexesResponse : ResultsResponse<PutIndexResult>
    {
    }

    internal class GetIndexesResponse : ResultsResponse<IndexDefinition>
    {
    }

    internal class GetTransformersResponse : ResultsResponse<TransformerDefinition>
    {
    }

    internal class GetIndexStatisticsResponse : ResultsResponse<IndexStats>
    {
    }

    internal class GetCertificatesResponse : ResultsResponse<CertificateDefinition>
    {
    }
}