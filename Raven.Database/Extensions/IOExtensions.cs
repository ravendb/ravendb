using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Raven.Database.Extensions
{
    public static class IOExtensions
    {
        public static string ToFullPath(this string path)
        {
            return Path.IsPathRooted(path) ? path : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, path);
        }
    }
}
