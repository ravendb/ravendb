using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class HiLoReturnCommand : RavenCommand
    {
        private readonly string _tag;
        private readonly long _last;
        private readonly long _end;
        private readonly int? _shardIndex;

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

        public HiLoReturnCommand(string tag, long last, long end, int? shardIndex) : this(tag, last, end)
        {
            _shardIndex = shardIndex;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var builder = new StringBuilder();

            builder.Append(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/hilo/return?tag=")
                .Append(_tag)
                .Append("&end=")
                .Append(_end)
                .Append("&last=")
                .Append(_last);

            if (_shardIndex.HasValue)
                builder.Append("&shardIndex=").Append(_shardIndex);

            url = builder.ToString();

            return new HttpRequestMessage
            {
                Method = HttpMethod.Put
            };
        }
    }
}
