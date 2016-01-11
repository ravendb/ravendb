using System.IO;
using System.Linq;
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
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);

            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                var sizeInBytes = employee.SizeInBytes;
                ptr = unmanagedPool.GetMemory(sizeInBytes, out size);
                employee.CopyTo(ptr);

                Parallel.ForEach(Enumerable.Range(0, 100), x =>
                {
                    using (var localCtx = new RavenOperationContext(unmanagedPool))
                        AssertComplexEmployee(str, ptr, sizeInBytes, localCtx);
                });
            }
        }
    }
}
