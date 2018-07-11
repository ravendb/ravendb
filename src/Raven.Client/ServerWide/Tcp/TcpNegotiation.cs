using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Tcp
{
    public class TcpNegotiation
    {
        public static int NegotiateProtocolVersion(JsonOperationContext documentsContext, Stream stream, TcpNegotiateParamaters parameters)
        {
            using (var writer = new BlittableJsonTextWriter(documentsContext, stream))
            {
                var currentVersion = parameters.Version;
                while (true)
                {
                    documentsContext.Write(writer, new DynamicJsonValue
                    {
                        [nameof(TcpConnectionHeaderMessage.DatabaseName)] = parameters.Database, // _parent.Database.Name,
                        [nameof(TcpConnectionHeaderMessage.Operation)] = parameters.Operation.ToString(),
                        [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = parameters.NodeTag,
                        [nameof(TcpConnectionHeaderMessage.OperationVersion)] = currentVersion
                    });
                    writer.Flush();
                    var version = parameters.ReadRespondAndGetVersion(documentsContext, writer);
                    var (supported, prevSupported) = TcpConnectionHeaderMessage.OperationVersionSupported(parameters.Operation, version);
                    if (supported)
                        return version;
                    if (prevSupported == -1)
                        return -1;
                    currentVersion = prevSupported;
                }
            }
        }
    }
    public class TcpNegotiateParamaters
    {
        public TcpConnectionHeaderMessage.OperationTypes Operation { get; set; }
        public int Version { get; set; }
        public string Database { get; set; }
        public string NodeTag { get; set; }

        public Func<JsonOperationContext, BlittableJsonTextWriter, int> ReadRespondAndGetVersion { get; set; }
    }
}
