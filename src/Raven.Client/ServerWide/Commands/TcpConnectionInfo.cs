using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Commands
{
    public class TcpConnectionInfo
    {
        public int Port;
        public string Url;
        public string Certificate;
        public string[] TcpServerUrls;
        public DynamicJsonValue ToJson()
        {
            var res =  new DynamicJsonValue
            {
                [nameof(Port)] = Port,
                [nameof(Url)] = Url,
                [nameof(Certificate)] = Certificate
            };

            var array = new DynamicJsonArray();
            foreach (var url in TcpServerUrls)
            {
                array.Add(url);
            }

            res[nameof(TcpServerUrls)] = array;

            return res;
        }
    }
}
