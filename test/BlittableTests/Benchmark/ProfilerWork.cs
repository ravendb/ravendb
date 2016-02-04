// -----------------------------------------------------------------------
//  <copyright file="ProfilerWork.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
//using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Json;

namespace NewBlittable.Tests.Benchmark
{
    public class ProfilerWork
    {
        public static async Task Run(int take)
        {
            string directory = @"C:\Users\bumax_000\Downloads\JsonExamples";
            var files = Directory.GetFiles(directory, "*.json");
            using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty))
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            {
                foreach (var file in files.OrderBy(x=> new FileInfo(x).Length).Take(take))
                {
                    var v = File.ReadAllBytes(file);
                    using (await blittableContext.Read(new MemoryStream(v), "doc1"))
                    {
                    }
                }
            }

        }
    }
}