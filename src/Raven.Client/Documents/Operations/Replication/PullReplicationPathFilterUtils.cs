using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.Replication
{
    internal static class PullReplicationPathFilterUtils
    {
        public static string[] NormalizeAndValidate(string[] allowedPaths, string name)
        {
            var normalizedPaths = Normalize(allowedPaths);
            Validate(normalizedPaths, name);
            return normalizedPaths;
        }

        public static string[] Normalize(string[] allowedPaths)
        {
            if (allowedPaths == null)
                return null;

            List<string> normalized = null;

            foreach (var path in allowedPaths)
            {
                var normalizedPath = path?.Trim();
                if (string.IsNullOrEmpty(normalizedPath))
                    continue;

                normalized ??= new List<string>(allowedPaths.Length);
                normalized.Add(normalizedPath);
            }

            return normalized?.ToArray() ?? [];
        }

        private static void Validate(string[] allowedPaths, string name)
        {
            if ((allowedPaths?.Length ?? 0) == 0)
                return;

            foreach (var path in allowedPaths)
            {
                if (path[path.Length - 1] != '*')
                    continue;

                if (path.Length > 1 && path[path.Length - 2] != '/' && path[path.Length - 2] != '-')
                    throw new InvalidOperationException(
                        $"When using '*' at the end of the allowed path, the previous character must be '/' or '-', but got: {path} for {name}");
            }
        }
    }
}
