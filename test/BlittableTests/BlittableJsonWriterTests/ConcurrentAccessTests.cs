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
    public unsafe class ConcurrentAccessTests: BlittableJsonTestBase
    {
        [Fact]
        public void ConcurrentReadsTest()
        {
            byte* ptr;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty);

            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
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
}
