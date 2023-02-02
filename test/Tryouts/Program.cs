using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils.MicrosoftLogging;
using Sparrow.Logging;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }
        
        private static int i1 = 1203;
        private static int i2 = 1204;
        private static int i3 = 1205;
        private static int i4 = 1206;
        private static int i5 = 1207;
        
        public static async Task Main(string[] args)
        {
            using var stream = new MemoryStream();
            LoggingSource.Instance.AttachPipeSink(stream);
            var logger = LoggingSource.Instance.GetLogger("HaludiSource", "HaludiLogger");
            
            logger.InfoDirectlyToStream((s, t) => s.InterpolateDirectly($"{t.i1}aaaaaaaaaaaaaaaa{t.i2}aaaaaaaaaaaaaaaaaa{t.i3}aaaaaaaaaaaaaaaaaaa{t.i4}aaaaaaaaaa{t.i5}aaaaaaaaaaaa"),
                (i1, i2, i3, i4, i5)
            );
            logger.UseArrayPool($"{i1}aaaaaaaaaaaaaaaa{i2}aaaaaaaaaaaaaaaaaa{i3}aaaaaaaaaaaaaaaaaaa{i4}aaaaaaaaaa{i5}aaaaaaaaaaaa");
            //Use stack allocated buffer
            logger.UseArrayPool(stackalloc char[128], $"{i1}aaaaaaaaaaaaaaaa{i2}aaaaaaaaaaaaaaaaaa{i3}aaaaaaaaaaaaaaaaaaa{i4}aaaaaaaaaa{i5}aaaaaaaaaaaa");

            
            Thread.Sleep(10000);
            stream.Seek(0, SeekOrigin.Begin);
            using (var streamReader = new StreamReader(stream))
            {
                var a = streamReader.ReadToEnd();
                var b = 0;
            }        
        }
    }
}
