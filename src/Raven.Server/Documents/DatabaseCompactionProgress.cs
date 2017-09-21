using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class DatabaseCompactionProgress : DeterminateProgress
    {
        public string TreeName;
        public long TreeProgress;
        public long TreeTotal;
        public string Message;
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(TreeProgress)] = TreeProgress;
            json[nameof(TreeTotal)] = TreeTotal;
            json[nameof(Message)] = Message;
            json[nameof(TreeName)] = TreeName;
            
            return json;
        }
    }
}
