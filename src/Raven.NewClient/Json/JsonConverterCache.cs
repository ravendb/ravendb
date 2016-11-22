using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Raven.Abstractions.Json
{
    public static class JsonConverterCache
    {
        private static CompoundKeyEqualityComparer Comparer = new CompoundKeyEqualityComparer();

        private class CompoundKey
        {
            internal readonly int HashKey;

            public CompoundKey( int hashKey )
            {
                this.HashKey = hashKey;
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


        [ThreadStatic]
        private static Dictionary<CompoundKey, JsonConverter> _cache;

        private static Dictionary<CompoundKey, JsonConverter> Cache
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get 
            { 
                if ( _cache == null )
                {
                    // While a race condition can happen here, we don't care as it will eventually create a single useless cache.
                    _cache = new Dictionary<CompoundKey, JsonConverter>(Comparer);
                }

                return _cache; 
            }
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

                // The locking will prevent the original thread to be able to continue until the one that got it releases it
                // With a lockless implementation we have found non-reproducible NullReferenceExceptions. The hypothesis is that task stealing 
                // (lightweight threading) may be the culprit. I prefer to pay 10% in performance here than fail or mask the error with extra indirections.
                var cache = Cache;
                lock ( cache )
                {
                    if (!cache.TryGetValue(key, out converter))
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
                        cache[newKey] = converter;
                    }
                }

                return converter;
            }            
        }


        private class CompoundKeyEqualityComparer : IEqualityComparer<CompoundKey>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(CompoundKey x, CompoundKey y)
            {
                if (x == null || y == null)
                    return false;

                SlowCompoundKey k;
                FastCompoundKey @this;
                if (x is FastCompoundKey)
                {
                    @this = x as FastCompoundKey;
                    k = y as SlowCompoundKey;
                }
                else
                {
                    @this = y as FastCompoundKey;
                    k = x as SlowCompoundKey;
                }

                JsonConverterCollection kCollection;
                if (!k.Collection.TryGetTarget(out kCollection))
                    return false;

                return @this.Collection == kCollection;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetHashCode(CompoundKey obj)
            {
                return obj.HashKey;
            }
        }
    }
}
