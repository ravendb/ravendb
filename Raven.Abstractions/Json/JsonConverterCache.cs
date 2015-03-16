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
        private static CompoundKeyEqualityComparer Comparer = new CompoundKeyEqualityComparer();

        private abstract class CompoundKey
        {
            internal readonly int HashKey;

	        protected CompoundKey( int hashKey )
            {
                this.HashKey = hashKey;
            }

	        public abstract JsonConverterCollection Collection { get;  }
        }

        private sealed class FastCompoundKey : CompoundKey
        {
            private readonly JsonConverterCollection collection;

            public FastCompoundKey(Type type, JsonConverterCollection collection)
                : base(type.GetHashCode() * 17 ^ collection.GetHashCode())
            {
                Debug.Assert(collection.IsFrozen);

                this.collection = collection;
            }

	        public override JsonConverterCollection Collection
	        {
		        get { return collection; }
	        }
        }

        private sealed class SlowCompoundKey : CompoundKey
        {
            private readonly WeakReference<JsonConverterCollection> collection;

            public SlowCompoundKey(Type type, JsonConverterCollection collection)
                : base(type.GetHashCode() * 17 ^ collection.GetHashCode())
            {
                Debug.Assert(collection.IsFrozen);

                this.collection = new WeakReference<JsonConverterCollection>(collection);
            }


			public override JsonConverterCollection Collection
			{
				get
				{
					JsonConverterCollection target;
					collection.TryGetTarget(out target);
					return target;
				}
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
                    _cache = new Dictionary<CompoundKey, JsonConverter>(Comparer);

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


        private class CompoundKeyEqualityComparer : IEqualityComparer<CompoundKey>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(CompoundKey x, CompoundKey y)
            {
                if (x == null || y == null)
                    return false;


                return x.Collection == y.Collection;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetHashCode(CompoundKey obj)
            {
                return obj.HashKey;
            }
        }
    }
}
