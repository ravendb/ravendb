using CdcSinkConfiguration = Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration;

namespace Raven.Server.Documents.CdcSink.Test
{
    public class TestCdcSinkScript
    {
        public CdcSinkConfiguration Configuration;

        public string Message { get; set; }
    }
}
