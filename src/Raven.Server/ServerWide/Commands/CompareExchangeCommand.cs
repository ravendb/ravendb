using System;
using System.IO;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class CompareExchangeCommand : CommandBase
    {
        public string Key;
        public BlittableJsonReaderObject Value;
        public long Index;

        public CompareExchangeCommand(){ }

        public CompareExchangeCommand(string key, BlittableJsonReaderObject value, long index)
        {
            if(string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key),"The key argument must have value");
            if(index < 0)
                throw new InvalidDataException("Index must be a non-negative number");

            Key = key;
            Value = value;
            Index = index;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(Key)] = Key;
            json[nameof(Value)] = Value;
            json[nameof(Index)] = Index;
            return json;
        }
    }
}
