using System;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents
{
    public class Attachment
    {
        public long Etag;
        public LazyStringValue LoweredDocumentId;
        public LazyStringValue LoweredName;
        public long StorageId;

        public DateTime LastModified;

        public LazyStringValue Name;
        public LazyStringValue ContentType;

        public Slice StreamIdentifier;

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