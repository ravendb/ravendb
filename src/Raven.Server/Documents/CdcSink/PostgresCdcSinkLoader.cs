using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.CdcSink;

public class PostgresCdcSinkLoader : CdcSinkLoader
{
    public PostgresCdcSinkLoader(DocumentDatabase database, ServerStore serverStore)
        : base(database, serverStore)
    {
    }

    protected override CdcSinkProcess CreateProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
    {
        return new PostgresCdcSinkProcess(configuration, database);
    }
}
