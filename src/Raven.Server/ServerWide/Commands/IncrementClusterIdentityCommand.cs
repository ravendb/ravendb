using Raven.Client;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class IncrementClusterIdentityCommand : UpdateValueForDatabaseCommand
    {
        private long _identity;

        private string _itemId;

        public string Prefix { get; set; }

        public IncrementClusterIdentityCommand()
            : base(null)
        {
            // for deserialization
        }

        public IncrementClusterIdentityCommand(string databaseName, string prefix)
            : base(databaseName)
        {
            Prefix = prefix;
        }

        public override string GetItemId()
        {
            return _itemId ?? (_itemId = $"{Constants.Documents.IdentitiesPrefix}{DatabaseName.ToLowerInvariant()}");
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, bool isPassive)
        {
            if (existingValue == null)
            {
                var djv = new DynamicJsonValue
                {
                    [Prefix] = _identity = 1
                };

                return context.ReadObject(djv, GetItemId());
            }

            existingValue.TryGet(Prefix, out long value);

            if (existingValue.Modifications == null)
                existingValue.Modifications = new DynamicJsonValue();

            existingValue.Modifications[Prefix] = _identity = value + 1;

            return context.ReadObject(existingValue, GetItemId());
        }

        public override object GetResult()
        {
            return _identity;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(Prefix)] = Prefix;
        }
    }
}
