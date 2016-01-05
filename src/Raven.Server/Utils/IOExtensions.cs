using System;
using System.IO;

namespace Raven.Server.Utils
{
    public static class IOExtensions
    {

        public static string ToFullPath(this string path, string basePath = null)
        {
            if (String.IsNullOrWhiteSpace(path))
                return String.Empty;
            path = Environment.ExpandEnvironmentVariables(path);
            if (path.StartsWith(@"~\") || path.StartsWith(@"~/"))
            {
                if (!string.IsNullOrEmpty(basePath))
                    basePath = Path.GetDirectoryName(basePath.EndsWith("\\") ? basePath.Substring(0, basePath.Length - 2) : basePath);

                path = Path.Combine(basePath ?? AppContext.BaseDirectory, path.Substring(2));
            }

            return Path.IsPathRooted(path) ? path : Path.Combine(basePath ?? AppContext.BaseDirectory, path);
        }

    }
}