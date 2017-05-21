using System;
using System.Net.Http;
using Raven.Client.Http;

namespace Raven.Client.Documents.Commands
{
    public class HiLoReturnCommand : RavenCommand
    {
        private readonly string _tag;
        private readonly long _last;
        private readonly long _end;

        public HiLoReturnCommand(string tag, long last, long end)
        {
            if (last < 0)
                throw new ArgumentOutOfRangeException(nameof(last));
            if (end < 0)
                throw new ArgumentOutOfRangeException(nameof(end));

            _tag = tag ?? throw new ArgumentNullException(nameof(tag));
            _last = last;
            _end = end;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/hilo/return?tag={_tag}&end={_end}&last={_last}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Put
            };
        }
    }
}
