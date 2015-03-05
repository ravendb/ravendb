using Raven.Imports.Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Abstractions.Json
{
    public static class JsonConverterCache
    {
        private class TypeCache
        {
            public Dictionary<Type, JsonConverter> Converter = new Dictionary<Type, JsonConverter>();
        }

        private static ThreadLocal<Dictionary<JsonConverterCollection, TypeCache>> _Cache = new ThreadLocal<Dictionary<JsonConverterCollection, TypeCache>>(() => new Dictionary<JsonConverterCollection, TypeCache>());

        private static Dictionary<JsonConverterCollection, TypeCache> Cache 
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
                JsonConverter converter = null;         
                TypeCache typeCache = null;

                if (!Cache.TryGetValue(converters, out typeCache))
                {
                    typeCache = new TypeCache();
                    Cache[converters] = typeCache;
                }

                if (!typeCache.Converter.TryGetValue(type, out converter))
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

                    typeCache.Converter[type] = converter;
                }

                return converter;
            }

            
        }
    }
}
