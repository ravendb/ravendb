using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;

namespace Raven.Client.Documents.Operations
{
    internal abstract class ResultsResponse<T>
    {
        public T[] Results { get; set; }
    }

    internal class GetIndexNamesResponse : ResultsResponse<string>
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

    internal class GetServerWideBackupConfigurationsResponse : ResultsResponse<ServerWideBackupConfiguration>
    {
    }

    internal class GetServerWideExternalReplicationsResponse : ResultsResponse<ServerWideExternalReplication>
    {
    }
}
