// -----------------------------------------------------------------------
//  <copyright file="AssemblyExtractor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;

using Lucene.Net.Documents;

using Raven.Abstractions;
using Raven.Abstractions.Data;

namespace Raven.Database.Server
{
    public static class AssemblyExtractor
    {
        private const string AssemblySuffix = ".dll";

        private const string CompressedAssemblySuffix = AssemblySuffix + ".zip";

        public static void ExtractEmbeddedAssemblies()
        {
            var assemblies = new HashSet<string> { 
                typeof(SystemTime).Assembly.GetName().Name, 
                typeof(Field).Assembly.GetName().Name 
            };

            var assembly = Assembly.GetExecutingAssembly();
            var assemblyLocation = Path.Combine(Path.GetDirectoryName(assembly.Location), Constants.AssembliesDirectoryName + "\\");
            var assembliesToExtract = FindAssembliesToExtract(assembly, assemblies);

          
            Extract(assembly, assembliesToExtract, assemblyLocation);

            foreach (var assemblyToExtract in assembliesToExtract)
                assemblies.Remove(assemblyToExtract.Value.Name);

#if !DEBUG
            if (assemblies.Count != 0)
                throw new InvalidOperationException("Not all embedded assemblies were extracted. Probably a bug.");
#endif
        }

        private static Dictionary<string, AssemblyToExtract> FindAssembliesToExtract(Assembly currentAssembly, HashSet<string> assembliesToFind)
        {
            var resources = currentAssembly.GetManifestResourceNames();
            var assembliesToExtract = new Dictionary<string, AssemblyToExtract>();

            foreach (var resource in resources)
            {
                if (!resource.StartsWith("costura"))
                    continue;

                var compressed = false;

                var assembly = assembliesToFind.FirstOrDefault(x => resource.EndsWith(x + CompressedAssemblySuffix, StringComparison.InvariantCultureIgnoreCase));
                if (assembly != null)
                    compressed = true;
                else
                    assembly = assembliesToFind.FirstOrDefault(x => resource.EndsWith(x + AssemblySuffix, StringComparison.InvariantCultureIgnoreCase));

                if (assembly == null)
                    continue;

                assembliesToExtract.Add(resource, new AssemblyToExtract { Compressed = compressed, Name = assembly });
            }

            return assembliesToExtract;
        }

        private static void Extract(Assembly assemblyToExtractFrom, IEnumerable<KeyValuePair<string, AssemblyToExtract>> assembliesToExtract, string location)
        {
            foreach (var assemblyToExtract in assembliesToExtract)
            {
				if (!Directory.Exists(location))
					Directory.CreateDirectory(location);

                using (var stream = assemblyToExtractFrom.GetManifestResourceStream(assemblyToExtract.Key))
                {
                    if (stream == null)
                        throw new InvalidOperationException("Could not extract assembly " + assemblyToExtract.Key + " from resources.");

                    var assemblyPath = Path.Combine(location, assemblyToExtract.Value.Name + AssemblySuffix);
                    if (File.Exists(assemblyPath))
                        File.Delete(assemblyPath);

                    if (assemblyToExtract.Value.Compressed)
                    {
                        using (var deflateStream = new DeflateStream(stream, CompressionMode.Decompress))
                        using (var file = File.Create(assemblyPath))
                            deflateStream.CopyTo(file);
                    }
                    else
                    {
                        using (var file = File.Create(assemblyPath))
                            stream.CopyTo(file);
                    }
                }
            }
        }

        private class AssemblyToExtract
        {
            public string Name { get; set; }

            public bool Compressed { get; set; }
        }
    }
}