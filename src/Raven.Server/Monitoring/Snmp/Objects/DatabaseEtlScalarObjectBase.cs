using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects;

public abstract class DatabaseEtlScalarObjectBase<TData> : DatabaseScalarObjectBase<TData>
    where TData : ISnmpData
{
    protected readonly string EtlName;

    protected DatabaseEtlScalarObjectBase(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int etlIndex, string dots)
        : base(databaseName, landlord, string.Format(dots, databaseIndex), etlIndex)
    {
        EtlName = etlName;
    }

    protected EtlProcess GetEtl(DocumentDatabase database)
    {
        return database.EtlLoader.Processes.SingleOrDefault(x => x.Name == EtlName);
    }
}
