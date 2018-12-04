using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class GetRawStreamResultCommand : RavenCommand<Stream>, IDisposable
    {
        private readonly string _commandUrl;
        private readonly HttpMethod _method;
        private readonly Stream _headerStream;
        private readonly bool _ownsHeaderStream;

        public GetRawStreamResultCommand(string commandUrl, Stream headerStream = null, HttpMethod method = null, bool ownsHeaderStream = false)
        {
            _commandUrl = commandUrl;
            if (string.IsNullOrWhiteSpace(_commandUrl))
                throw new ArgumentException("command path should not be empty",nameof(commandUrl));

            _headerStream = headerStream;
            _ownsHeaderStream = ownsHeaderStream;
            _method = method ?? HttpMethod.Get;
            ResponseType = RavenCommandResponseType.Raw;
        }

        public override bool IsReadRequest => true;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = _commandUrl.StartsWith("/") ? 
                $"{node.Url}{_commandUrl}" : 
                $"{node.Url}/{_commandUrl}";

            var requestMessage = new HttpRequestMessage
            {
                Method = _method
                
            };

            if (_headerStream == null)
                return requestMessage;

            if(_headerStream.CanSeek) //just to make sure
                _headerStream.Position = 0; 

            requestMessage.Content = new StreamContent(_headerStream);

            return requestMessage;
        }

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            Result = context.CreateMemoryStream();
            stream.CopyTo(Result);
        }

        public void Dispose()
        {
            if (_ownsHeaderStream)
            {
                _headerStream?.Dispose();
            }

            Result.Dispose();
        }
    }
}
