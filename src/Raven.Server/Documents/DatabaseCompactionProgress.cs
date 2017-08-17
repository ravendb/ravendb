using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class DatabaseCompactionProgress : IOperationProgress
    {
        public string ObjectType;
        public long GlobalProgress;
        public long GlobalTotal;
        public string ObjectName;
        public long ObjectProgress;
        public long ObjectTotal;
        public string Message;

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(ObjectType)] = ObjectType,
                [nameof(GlobalProgress)] = GlobalProgress,
                [nameof(GlobalTotal)] = GlobalTotal,
                [nameof(ObjectName)] = ObjectName,
                [nameof(ObjectProgress)] = ObjectProgress,
                [nameof(ObjectTotal)] = ObjectTotal,
                [nameof(Message)] = Message
            };
        }
    }
}
