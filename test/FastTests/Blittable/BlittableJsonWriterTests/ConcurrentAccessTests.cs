using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json;
using Xunit;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public class ConcurrentAccessTests : BlittableJsonTestBase
    {
        [Fact]
        public void ConcurrentReadsTest()
        {
            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            using (var employee = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                /* FileStream file = SafeFileStream.Create(@"c:\Temp\example.txt",FileMode.Create);
                 employee.WriteTo(file);
                 file.Flush();
                 file.Dispose();*/
                AssertEmployees(employee, str);
            }
        }

        private static unsafe void AssertEmployees(BlittableJsonReaderObject employee, string str)
        {
            var basePointer = employee.BasePointer;
            var size = employee.Size;

            Parallel.ForEach(Enumerable.Range(0, 100), RavenTestHelper.DefaultParallelOptions, x =>
             {
                 using (var localCtx = JsonOperationContext.ShortTermSingleUse())
                 {
                     AssertComplexEmployee(str, new BlittableJsonReaderObject(basePointer, size, localCtx), localCtx);
                 }
             });
        }
    }
}
