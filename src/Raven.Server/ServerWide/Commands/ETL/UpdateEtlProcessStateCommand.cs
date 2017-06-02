using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public class UpdateEtlProcessStateCommand : UpdateValueForDatabaseCommand
    {
        public string Destination { get; set; }

        public string TransformationName { get; set; }

        public long LastProcessedEtag { get; set; }

        public ChangeVectorEntry[] ChangeVector { get; set; }

        public string NodeTag { get; set; }

        private UpdateEtlProcessStateCommand() : base(null)
        {
            // for deserialization
        }

        public UpdateEtlProcessStateCommand(string databaseName, string destination, string transformationName, long lastProcessedEtag, ChangeVectorEntry[] changeVector,
            string nodeTag) : base(databaseName)
        {
            Destination = destination;
            TransformationName = transformationName;
            LastProcessedEtag = lastProcessedEtag;
            ChangeVector = changeVector;
            NodeTag = nodeTag;
        }

        public override string GetItemId()
        {
            return EtlProcessState.GenerateItemName(DatabaseName, Destination, TransformationName);
        }

        public override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            EtlProcessState state;

            if (existingValue != null)
                state = JsonDeserializationClient.EtlProcessState(existingValue);
            else
            {
                state = new EtlProcessState
                {
                    Destination = Destination,
                    TransformationName = TransformationName
                };
            }

            state.LastProcessedEtagPerNode[NodeTag] = LastProcessedEtag;
            state.ChangeVector = ChangeVector;


            return context.ReadObject(state.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Destination)] = Destination;
            json[nameof(TransformationName)] = TransformationName;
            json[nameof(LastProcessedEtag)] = LastProcessedEtag;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(ChangeVector)] = ChangeVector?.ToJson();
        }
    }
}