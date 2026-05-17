using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Monitoring.Snmp;

public abstract class UpdateSnmpDatabaseMappingCommand : UpdateValueForDatabaseCommand
{
    private string _itemId;

    protected abstract List<string> Items { get; }

    protected abstract string ItemsPropertyName { get; }

    protected abstract string GetStorageKeyForDatabase(string databaseName);

    protected UpdateSnmpDatabaseMappingCommand()
    {
        // for deserialization
    }

    protected UpdateSnmpDatabaseMappingCommand(string databaseName, string uniqueRequestId)
        : base(databaseName, uniqueRequestId)
    {
    }

    public sealed override string GetItemId()
    {
        return _itemId ??= GetStorageKeyForDatabase(DatabaseName);
    }

    public sealed override void FillJson(DynamicJsonValue json)
    {
        if (Items == null)
            return;

        json[ItemsPropertyName] = new DynamicJsonArray(Items);
    }

    protected sealed override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject previousValue)
    {
        if (previousValue != null)
        {
            previousValue.Modifications ??= new DynamicJsonValue();

            AddItemsIfNecessary(previousValue.Modifications, previousValue, Items);

            if (previousValue.Modifications.Properties.Count == 0)
            {
                return new UpdatedValue(UpdatedValueActionType.Noop, value: null);
            }

            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(previousValue, GetItemId()));
        }

        var djv = new DynamicJsonValue();
        AddItemsIfNecessary(djv, null, Items);

        return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(djv, GetItemId()));
    }

    private static void AddItemsIfNecessary(DynamicJsonValue djv, BlittableJsonReaderObject previousValue, List<string> items)
    {
        if (items == null)
            return;

        var propertiesCount = previousValue?.Count ?? 0;
        foreach (var item in items)
        {
            if (previousValue == null || previousValue.TryGet(item, out long _) == false)
                djv[item] = propertiesCount + djv.Properties.Count + 1;
        }
    }
}

