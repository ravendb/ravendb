using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public class ConcurrentAccessTests : BlittableJsonTestBase
    {
        public ConcurrentAccessTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ConcurrentReadsTest()
        {
            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            using (var employee = blittableContext.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                /* FileStream file = SafeFileStream.Create(@"c:\Temp\example.txt",FileMode.Create);
                 employee.WriteTo(file);
                 file.Flush();
                 file.Dispose();*/
                AssertEmployees(employee, str);
            }
        }

        [Fact]
        public void ConcurrentWrite_WhenResetCachedPropertiesForNewDocument_ShouldThrowInformativeException()
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var writer = new BlittableJsonWriter(context))
                using (var secondWriter = new BlittableJsonWriter(context))
                {
                    secondWriter.WriteStartObject();
                    secondWriter.WritePropertyName("Property1");
                    secondWriter.WriteValue(4);
                    secondWriter.WriteEndObject();

                    context.CachedProperties.NewDocument();
                    writer.WriteStartObject();
                    writer.WritePropertyName("ObjectProp");
                    writer.WriteValue(4);
                    writer.WriteEndObject();
                    writer.FinalizeDocument();
                    var first = writer.CreateReader();

                    secondWriter.FinalizeDocument();
                    var second = secondWriter.CreateReader();
                }
            });
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
