using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Server.Commands
{
    public class PutSecretKeyCommand : RavenCommand
    {
        private readonly string _name;
        private readonly string _base64Key;
        private readonly bool _overwrite;

        public PutSecretKeyCommand(
            string name, 
            string base64Key, 
            bool overwrite = false /*Be careful with this one, overwriting a key might be disastrous*/)
        {
            _overwrite = overwrite;
            _name = name;
            _base64Key = base64Key;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/secrets?name={_name}";

            if (_overwrite)
            {
                url += $"&overwrite={_overwrite}";
            }
            
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new StringContent(_base64Key)
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();
        }
        
        public override bool IsReadRequest => false;
    }
}