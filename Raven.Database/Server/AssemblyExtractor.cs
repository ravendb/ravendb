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
using Raven.Database.Linq;

namespace Raven.Database.Server
{
    public static class AssemblyExtractor
    {
        private const string AssembliesPath = "Assemblies";

        private const string AssemblySuffix = ".dll";

        private const string CompressedAssemblySuffix = AssemblySuffix + ".zip";

        public static void ExtractEmbeddedAssemblies()
        {
            var assemblies = new HashSet<string> { 
                typeof(SystemTime).Assembly.GetName().Name, 
                typeof(AbstractViewGenerator).Assembly.GetName().Name, 
                typeof(Field).Assembly.GetName().Name 
            };

            var entryAssembly = Assembly.GetEntryAssembly();
            var resources = entryAssembly.GetManifestResourceNames();

            InitializeDirectoryAndCleanup();

            var assembliesToExtract = new Dictionary<string, AssemblyToExtract>();

            foreach (var resource in resources)
            {
                if (!resource.StartsWith("costura"))
                    continue;

                var compressed = false;

                var assembly = assemblies.FirstOrDefault(x => resource.EndsWith(x + CompressedAssemblySuffix, StringComparison.InvariantCultureIgnoreCase));
                if (assembly != null) 
                    compressed = true;
                else
                    assembly = assemblies.FirstOrDefault(x => resource.EndsWith(x + AssemblySuffix, StringComparison.InvariantCultureIgnoreCase));

                if (assembly == null)
                    continue;

                assembliesToExtract.Add(resource, new AssemblyToExtract
                                                  {
                                                      Compressed = compressed,
                                                      Name = assembly
                                                  });
            }

            Extract(entryAssembly, assembliesToExtract);
        }

        private static void Extract(Assembly assemblyToExtractFrom, IEnumerable<KeyValuePair<string, AssemblyToExtract>> assembliesToExtract)
        {
            foreach (var assemblyToExtract in assembliesToExtract)
            {
                using (var stream = assemblyToExtractFrom.GetManifestResourceStream(assemblyToExtract.Key))
                {
                    if (stream == null) 
                        throw new InvalidOperationException("Could not extract assembly " + assemblyToExtract.Key + " from resources.");

                    var assemblyPath = AssembliesPath + "/" + assemblyToExtract.Value.Name + AssemblySuffix;

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

        private static void InitializeDirectoryAndCleanup()
        {
            if (!Directory.Exists(AssembliesPath))
            {
                Directory.CreateDirectory(AssembliesPath);
                return;
            }

            foreach (var file in Directory.GetFiles(AssembliesPath))
                File.Delete(file);
        }

        private class AssemblyToExtract
        {
            public string Name { get; set; }

            public bool Compressed { get; set; }
        }
    }
}