using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Commands
{
    public class TcpConnectionInfo
    {
        public int Port;
        public string Url;
        public string Certificate;
        public string[] ServerUrls;
        public DynamicJsonValue ToJson()
        {
            var res =  new DynamicJsonValue
            {
                [nameof(Port)] = Port,
                [nameof(Url)] = Url,
                [nameof(Certificate)] = Certificate
            };
            if (ServerUrls == null)
                return res;

            var array = new DynamicJsonArray();
            foreach (var url in ServerUrls)
            {
                array.Add(url);
            }

            res[nameof(ServerUrls)] = array;

            return res;
        }
    }
}
