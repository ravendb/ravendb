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
        private class CompoundKey
        {
            private readonly int hashKey;

            public CompoundKey( int hashKey )
            {
                this.hashKey = hashKey;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public sealed override int GetHashCode()
            {
                return hashKey;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public sealed override bool Equals(Object obj)
            {
                if (obj == null)
                    return false;

                SlowCompoundKey k;
                FastCompoundKey @this;
                if (obj.GetType() == typeof(FastCompoundKey))
                {
                    @this = obj as FastCompoundKey;
                    k = this as SlowCompoundKey;
                }
                else
                {
                    @this = this as FastCompoundKey;
                    k = obj as SlowCompoundKey;
                }


                JsonConverterCollection kCollection;
                if (!k.Collection.TryGetTarget(out kCollection))
                    return false;

                return @this.Collection == kCollection;
            }
        }

        private sealed class FastCompoundKey : CompoundKey
        {
            public readonly Type Type;
            public readonly JsonConverterCollection Collection;

            public FastCompoundKey(Type type, JsonConverterCollection collection)
                : base(type.GetHashCode() * 17 + collection.GetHashCode())
            {
                Debug.Assert(collection.IsFrozen);

                this.Type = type;
                this.Collection = collection;
            }
        }

        private sealed class SlowCompoundKey : CompoundKey
        {
            public readonly Type Type;
            public readonly WeakReference<JsonConverterCollection> Collection;

            public SlowCompoundKey(Type type, JsonConverterCollection collection)
                : base(type.GetHashCode() * 17 + collection.GetHashCode())
            {
                Debug.Assert(collection.IsFrozen);

                this.Type = type;
                this.Collection = new WeakReference<JsonConverterCollection>(collection);
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
                var key = new FastCompoundKey(type, converters);

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

                    var newKey = new SlowCompoundKey(type, converters);
                    Cache[newKey] = converter;
                }

                return converter;
            }

            
        }
    }
}
