using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands
{
    public class ResolveConflictCommand : RavenCommand<ResolveConflictResult>
    {
        private readonly string _id;
        private readonly ChangeVectorEntry[] _changeVector;
        public override bool IsReadRequest => false;
        private readonly JsonOperationContext _context;

        public ResolveConflictCommand(string id,ChangeVectorEntry[] changeVector, JsonOperationContext context)
        {
            _id = id;
            _changeVector = changeVector;
            _context = context;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/conflicts?docId={_id}";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(_context,stream))
                    {
                        _context.Write(writer, new DynamicJsonValue
                        {
                            ["ChangeVector"] = _changeVector.ToJson()
                        });
                        writer.Flush();
                    }
                })
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.ResolveConflictResult(response);
        }
    }
}
