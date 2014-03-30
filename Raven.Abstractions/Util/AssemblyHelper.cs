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

        public static string GetExtractedAssemblyLocationFor(Type type, Assembly executingAssembly)
        {
            if (File.Exists(type.Assembly.Location))
                return type.Assembly.Location;

            var path = Path.GetDirectoryName(executingAssembly.Location);
            var name = type.Assembly.GetName().Name;

            return Path.Combine(path, Constants.AssembliesDirectoryName, name + ".dll");
        }
    }
}