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
        public static TcpFeaturesSupported NegotiateProtocolVersion(JsonOperationContext documentsContext, Stream stream, TcpNegotiateParamaters parameters)
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
                    //In this case we usally throw internaly but for completeness we better handle it
                    if (version == -2)
                    {
                        return new TcpFeaturesSupported(parameters.Operation, -2);
                    }
                    var (supported, prevSupported) = TcpConnectionHeaderMessage.OperationVersionSupported(parameters.Operation, version);
                    if (supported)
                        return new TcpFeaturesSupported(parameters.Operation, version);
                    if (prevSupported == -1)
                        return new TcpFeaturesSupported(parameters.Operation, -1);
                    currentVersion = prevSupported;
                }
            }
        }
    }

    public class TcpFeaturesSupported
    {
        private int _version;
        private TcpConnectionHeaderMessage.OperationTypes _type;

        public TcpFeaturesSupported(TcpConnectionHeaderMessage.OperationTypes type, int protocolVersion)
        {
            _version = protocolVersion;
            _type = type;
        }

        public bool IsMissingAttachmentSupported {
            get
            {
                if (_type != TcpConnectionHeaderMessage.OperationTypes.Replication)
                    throw new InvalidOperationException($"Was asked about IsAttachmentSupported while this object represents {_type} operation and attachments" +
                                                        $" are only relevant to {TcpConnectionHeaderMessage.OperationTypes.Replication} protocols");
                switch (_version)
                {
                    case 33:
                        return true;
                    default:
                        return false;
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
