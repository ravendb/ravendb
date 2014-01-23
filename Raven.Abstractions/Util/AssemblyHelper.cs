// -----------------------------------------------------------------------
//  <copyright file="AssemblyHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

namespace Raven.Abstractions.Util
{
    public static class AssemblyHelper
    {
        public static string GetEmbeddedAssemblyLocationFor<T>()
        {
            return GetEmbeddedAssemblyLocationFor(typeof(T));
        }

        public static string GetEmbeddedAssemblyLocationFor(Type type)
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

        public static string GetExtractedAssemblyLocationFor<T>()
        {
            return GetExtractedAssemblyLocationFor(typeof(T));
        }

        public static string GetExtractedAssemblyLocationFor(Type type)
        {
#if SILVERLIGHT
            return type.Assembly.Location;
#else
            var assemblyPath = "Assemblies";
            var name = type.Assembly.GetName().Name;

            var path = assemblyPath + "/" + name + ".dll";
            return Path.GetFullPath(path);
#endif
        }
    }
}