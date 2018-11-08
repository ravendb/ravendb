using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Commands
{
    public class TcpConnectionInfo
    {
        public int Port;
        public string Url;
        public string Certificate;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Port)] = Port,
                [nameof(Url)] = Url,
                [nameof(Certificate)] = Certificate
            };
        }
    }
}
