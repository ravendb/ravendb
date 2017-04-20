using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.SQL;

namespace Raven.Server.Documents.ETL
{
    public class EtlDestinationsConfig
    {
        public List<EtlConfiguration<RavenDestination>> RavenDestinations = new List<EtlConfiguration<RavenDestination>>();

        public List<EtlConfiguration<SqlDestination>> SqlDestinations = new List<EtlConfiguration<SqlDestination>>();
    }
}