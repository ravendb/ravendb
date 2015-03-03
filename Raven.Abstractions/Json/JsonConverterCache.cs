using Raven.Imports.Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Abstractions.Json
{
    public static class JsonConverterCache
    {
        private sealed class CompoundKey
        {
            public readonly Type Type;
            public readonly JsonConverterCollection Collection;
            private readonly int hashKey;

            public CompoundKey(Type type, JsonConverterCollection collection)
            {
                Debug.Assert( collection.IsFrozen );

                this.Type = type;
                this.Collection = collection;

                this.hashKey = type.GetHashCode() * 17 + collection.GetHashCode();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public sealed override int GetHashCode()
            {
 	             return hashKey;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public sealed override bool Equals(Object obj)
            {
                CompoundKey k = obj as CompoundKey;

                // Check for null values and compare run-time types.
                if (k == null)
                    return false;
                
                return (Type == k.Type) && (Collection == k.Collection);
            }
        }

        private static ThreadLocal<Dictionary<CompoundKey, JsonConverter>> _Cache = new ThreadLocal<Dictionary<CompoundKey, JsonConverter>>(() => new Dictionary<CompoundKey, JsonConverter>());

        private static Dictionary<CompoundKey, JsonConverter> Cache
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _Cache.Value; }
        }

        public static JsonConverter GetMatchingConverter(JsonConverterCollection converters, Type type)
        {
            if (converters == null)
                return null;
             
            if ( !converters.IsFrozen )
            {
                int count = converters.Count;
                for (int i = 0; i < count; i++)
                {
                    var conv = converters[i];
                    if (conv.CanConvert(type))
                        return conv;
                }

                return null;
            }
            else
            {
                CompoundKey key = new CompoundKey(type, converters);

                JsonConverter converter;
                if (!Cache.TryGetValue(key, out converter))
                {
                    int count = converters.Count;
                    for (int i = 0; i < count; i++)
                    {
                        var conv = converters[i];
                        if (conv.CanConvert(type))
                        {
                            converter = conv;
                            break;
                        }
                    }

                    Cache[key] = converter;
                }

                return converter;
            }

            
        }
    }
}
