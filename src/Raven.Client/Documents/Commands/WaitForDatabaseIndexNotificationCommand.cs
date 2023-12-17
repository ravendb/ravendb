using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class WaitForDatabaseIndexNotificationCommand : RavenCommand
    {
        private readonly long _index;
        private readonly string _database;

        public WaitForDatabaseIndexNotificationCommand(long index, string database)
        {
            _index = index;
            _database = database;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{_database}/admin/rachis/wait-for-index-notification?index={_index}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();
        }

        public override bool IsReadRequest => true;
    }
}
