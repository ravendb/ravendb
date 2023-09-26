using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details;

public sealed class ComplexFieldsWarning : INotificationDetails
{
    public ComplexFieldsWarning()
    {
        //deserialization
    }

    public ComplexFieldsWarning(ConcurrentQueue<(string indexName, string fieldName)> complexFields)
    {
        Fields = new();

        while (complexFields.TryDequeue(out var field))
        {
            ref var indexFields = ref CollectionsMarshal.GetValueRefOrAddDefault(Fields, field.indexName, out var exists);
            if (exists == false)
                indexFields = new();

            indexFields.Add(field.fieldName);
        }
    }
    
    public Dictionary<string, List<string>> Fields { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        var djv = new DynamicJsonValue(GetType());
        var listOfFields = new DynamicJsonValue();

        foreach (var (indexName, indexComplexFields) in Fields)
        {
            var complexInIndex = new DynamicJsonArray();
            foreach (var field in indexComplexFields)
                complexInIndex.Add(field);

            listOfFields[indexName] = complexInIndex;
        }
        djv[nameof(Fields)] = listOfFields;

        return djv;
    }
}
