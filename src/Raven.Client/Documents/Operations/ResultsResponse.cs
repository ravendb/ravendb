using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.OngoingTasks;

namespace Raven.Client.Documents.Operations
{
    internal abstract class ResultsResponse<T>
    {
        public T[] Results { get; set; }
    }

    internal sealed class GetIndexNamesResponse : ResultsResponse<string>
    {
    }

    internal sealed class PutIndexesResponse : ResultsResponse<PutIndexResult>
    {
    }

    internal sealed class GetIndexesResponse : ResultsResponse<IndexDefinition>
    {
    }

    internal sealed class GetIndexStatisticsResponse : ResultsResponse<IndexStats>
    {
    }

    internal sealed class GetCertificatesResponse : ResultsResponse<CertificateDefinition>
    {
    }

    internal sealed class GetCertificatesMetadataResponse : ResultsResponse<CertificateMetadata>
    {
    }
    
    internal sealed class GetClientCertificatesResponse : ResultsResponse<CertificateRawData>
    {
    }

    internal sealed class GetServerWideBackupConfigurationsResponse : ResultsResponse<ServerWideBackupConfiguration>
    {
    }

    internal sealed class GetServerWideExternalReplicationsResponse : ResultsResponse<ServerWideExternalReplication>
    {
    }
}
