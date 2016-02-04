using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Server.Json;
using Xunit;


namespace BlittableTests.BlittableJsonWriterTests
{
    public class ConcurrentAccessTests: BlittableJsonTestBase
    {
        [Fact]
        public async Task ConcurrentReadsTest()
        {
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty);

            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = await blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
               /* FileStream file = new FileStream(@"c:\Temp\example.txt",FileMode.Create);
                employee.WriteTo(file);
                file.Flush();
                file.Dispose();*/
                AssertEmployees(employee, unmanagedPool, str);
            }
        }

        private static unsafe void AssertEmployees(BlittableJsonReaderObject employee, UnmanagedBuffersPool unmanagedPool, string str)
        {
            var basePointer = employee.BasePointer;
            var size = employee.Size;

            Parallel.ForEach(Enumerable.Range(0, 100), x =>
            {
                using (var localCtx = new RavenOperationContext(unmanagedPool))
                {
                    AssertComplexEmployee(str, new BlittableJsonReaderObject(basePointer, size, localCtx), localCtx);
                }
            });
        }
    }
}
