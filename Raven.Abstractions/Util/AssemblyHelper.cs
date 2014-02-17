#if !SILVERLIGHT
// -----------------------------------------------------------------------
//  <copyright file="AssemblyHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Reflection;

using Raven.Abstractions.Data;

namespace Raven.Abstractions.Util
{
    public static class AssemblyHelper
    {
        public static string GetAssemblyLocationFor<T>()
        {
            return GetAssemblyLocationFor(typeof(T));
        }

        public static string GetAssemblyLocationFor(Type type)
        {
            var assembly = type.Assembly;
            var location = assembly.Location;

            if (string.IsNullOrEmpty(location))
            {
                if (!string.IsNullOrEmpty(assembly.CodeBase) && assembly.CodeBase.StartsWith("file:///", StringComparison.OrdinalIgnoreCase))
                    location = assembly.CodeBase.Substring(8);
            }

            if (string.IsNullOrEmpty(location) || !File.Exists(location))
                throw new InvalidOperationException("Could not determine assembly location for: " + type.FullName);

            return location;
        }

        public static string GetExtractedAssemblyLocationFor<T>(Assembly executingAssembly)
        {
            return GetExtractedAssemblyLocationFor(typeof(T), executingAssembly);
        }

        public static string GetExtractedAssemblyLocationFor(Type type, Assembly executingAssembly)
        {
#if !DEBUG
            var path = Path.GetDirectoryName(executingAssembly.Location);
            var name = type.Assembly.GetName().Name;

            return Path.Combine(path, Constants.AssembliesDirectoryName + "\\", name + ".dll");
#else
            return type.Assembly.Location; // we do not merge/extract assemblies in DEBUG
#endif
        }
    }
}
#endif