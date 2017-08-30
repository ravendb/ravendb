using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateClusterIdentityCommand : UpdateValueForDatabaseCommand
    {
        private string _itemId;

        public Dictionary<string, long> Identities { get; set; }

        public UpdateClusterIdentityCommand()
            : base(null)
        {
        }

        public UpdateClusterIdentityCommand(string databaseName, Dictionary<string, long> identities)
            : base(databaseName)
        {
            Identities = new Dictionary<string, long>(identities);
        }

        public override string GetItemId()
        {
            return _itemId ?? (_itemId = $"{Constants.Documents.IdentitiesPrefix}{DatabaseName.ToLowerInvariant()}");
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, bool isPassive)
        {
            if (existingValue == null)
            {
                var djv = Identities.ToJson();
                return context.ReadObject(djv, GetItemId());
            }

            if (existingValue.Modifications == null)
                existingValue.Modifications = new DynamicJsonValue();

            foreach (var kvp in Identities)
            {
                if (existingValue.TryGet(kvp.Key, out long value) == false || kvp.Value > value)
                {
                    if (existingValue.Modifications == null)
                        existingValue.Modifications = new DynamicJsonValue();

                    existingValue.Modifications[kvp.Key] = kvp.Value;
                }
            }

            if (existingValue.Modifications != null)
                existingValue = context.ReadObject(existingValue, GetItemId());

            return existingValue;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Identities)] = (Identities ?? new Dictionary<string, long>()).ToJson();
        }
    }
}
