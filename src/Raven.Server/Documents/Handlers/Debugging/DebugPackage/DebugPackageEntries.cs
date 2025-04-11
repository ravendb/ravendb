using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageEntries
{
    public class Entry
    {
        private static JsonSerializerOptions DeserializeOptions = new JsonSerializerOptions { IncludeFields = true, Converters = { new JsonStringEnumConverter() } };
        
        public string Name { get; set; }
        
        public Stream Content { get; set; }
        
        public JsonDocument Json { get; set; }

        public bool TryGetJsonValue<T>(string name, out T value)
        {
            if (Json.RootElement.TryGetProperty(name, out var element) == false)
            {
                value = default;
                return false;
            }

            value = element.Deserialize<T>(DeserializeOptions);
            return true;
        }
        
        public bool TryGetJson(string name, out JsonElement element)
        {
            if (Json.RootElement.TryGetProperty(name, out element) == false)
            {
                element = default;
                return false;
            }

            return true;
        }
        
        public T Deserialize<T>()
        {
            return Json.RootElement.Deserialize<T>(DeserializeOptions);
        }
    }
    
    private Dictionary<string, Entry> _entries = new Dictionary<string, Entry>();

    public void Add(string entryName, Stream content, JsonDocument json)
    {
        _entries.Add(entryName, new Entry()
        {
            Name = entryName,
            Content = content,
            Json = json
        });
    }

    public bool TryGetValue<THandler, TValueType>(Expression<Func<THandler, object>> debugEndpoint, string fieldName, out TValueType value) where THandler : RequestHandler
    {
        if (TryGetEntry(debugEndpoint, out var entry) == false)
        {
            value = default;
            return false;
        }
        
        return entry.TryGetJsonValue(fieldName, out value);
    }
    
    public bool TryGetValue<THandler, TValueType>(Expression<Func<THandler, object>> debugEndpoint, out TValueType value) where THandler : RequestHandler
    {
        if (TryGetEntry(debugEndpoint, out var entry) == false)
        {
            value = default;
            return false;
        }

        value = entry.Deserialize<TValueType>();
        return true;
    }

    public bool TryGetEntry<T>(Expression<Func<T, object>> debugEndpoint, out Entry entry)
    {
        var entryName = DebugPackageExtensions.GetPackageEntryName(debugEndpoint);
        
        return _entries.TryGetValue(entryName, out entry);
    }

    public bool TryGetEntry(string path, string prefix, string extension, out Entry entry)
    {
        var entryName = DebugInfoPackageUtils.GetOutputPathFromRouteInformation(path, prefix, extension);
        
        return _entries.TryGetValue(entryName, out entry);
    }
    
    public bool TryGetValue<TValueType>(string path, out TValueType value)
    {
        if (TryGetEntry(path, string.Empty, "json", out var entry) == false)
        {
            value = default;
            return false;
        }

        value = entry.Deserialize<TValueType>();
        return true;
    }
}
