using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.CdcSink;

namespace Raven.Server.Monitoring.Snmp.Objects;

public abstract class DatabaseCdcSinkScalarObjectBase<TData> : DatabaseScalarObjectBase<TData>
    where TData : ISnmpData
{
    protected readonly string CdcSinkName;

    protected DatabaseCdcSinkScalarObjectBase(string databaseName, string cdcSinkName, DatabasesLandlord landlord, int databaseIndex, int cdcSinkIndex, string dots)
        : base(databaseName, landlord, string.Format(dots, databaseIndex), cdcSinkIndex)
    {
        CdcSinkName = cdcSinkName;
    }

    protected CdcSinkProcess GetCdcSink(DocumentDatabase database)
    {
        return database.CdcSinkLoader.Processes.SingleOrDefault(x => x.Name == CdcSinkName);
    }
}
