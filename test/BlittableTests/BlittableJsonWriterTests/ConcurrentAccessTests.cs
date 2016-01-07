using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConsoleApplication4;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Json;
using Xunit;

namespace NewBlittable.Tests.BlittableJsonWriterTests
{
    public unsafe class ConcurrentAccessTests: BlittableJsonTestBase
    {
        [Fact]
        public void ConcurrentReadsTest()
        {
            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);

            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(str)), blittableContext,
                "doc1"))
            {
                employee.Write();
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                Parallel.ForEach(Enumerable.Range(0, 100), x =>
                {
                    AssertComplexEmployee(str, ptr, employee, blittableContext);
                });
            }
        }
    }
}
