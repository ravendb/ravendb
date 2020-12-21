using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Commands
{
    public class TcpConnectionInfo
    {
        public int Port;
        public string Url;
        public string Certificate;
        public string[] Urls;
        public string NodeTag;

        public DynamicJsonValue ToJson()
        {
            var res =  new DynamicJsonValue
            {
                [nameof(Port)] = Port,
                [nameof(Url)] = Url,
                [nameof(Certificate)] = Certificate,
                [nameof(NodeTag)] = NodeTag
            };
            if (Urls == null)
                return res;

            var array = new DynamicJsonArray();
            foreach (var url in Urls)
            {
                array.Add(url);
            }

            res[nameof(Urls)] = array;

            return res;
        }
    }
}
