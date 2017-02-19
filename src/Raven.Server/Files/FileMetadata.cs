using System;
using Microsoft.Extensions.Primitives;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Files
{
    public class FileMetadata : IDisposable
    {
        public long Etag;
        public LazyStringValue Name;
        public LazyStringValue LoweredKey;
        public long StorageId;
        public BlittableJsonReaderObject Metadata;

        public DateTime LastModifed;

        public Slice StreamIdentifier;
        public IDisposable StreamIdentifierDispose;

        public string ContentType;

        public void Dispose()
        {
            Metadata?.Dispose();
            Name?.Dispose();
            LoweredKey?.Dispose();
            StreamIdentifierDispose?.Dispose();
        }

        public static string Canonize(string name, bool trimEnd = true)
        {
            if (string.IsNullOrWhiteSpace(name))
                return "/";

            name = Uri.UnescapeDataString(name);
            name = name.Replace("\\", "/");

            if (name.StartsWith("/") == false)
                name = name.Insert(0, "/");

            if (trimEnd)
                name = name.TrimEnd('/');

            return name;
        }
    }
}