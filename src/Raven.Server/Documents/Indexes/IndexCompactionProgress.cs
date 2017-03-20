using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes
{
    public class IndexCompactionProgress : DeterminateProgress
    {
        public string Message { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json["Message"] = Message;

            return json;
        }
    }
}