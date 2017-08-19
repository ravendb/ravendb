using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations.Certificates;

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

    internal class GetIndexStatisticsResponse : ResultsResponse<IndexStats>
    {
    }

    internal class GetCertificatesResponse : ResultsResponse<CertificateDefinition>
    {
    }

    internal class GetClientCertificatesResponse : ResultsResponse<CertificateRawData>
    {
    }
}
