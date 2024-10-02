using System.IO;

namespace Sparrow.Server.Platform.Posix;

internal interface ISmapsReader
{
    SmapsReadResult<T> CalculateMemUsageFromSmaps<T>() where T : struct, ISmapsReaderResultAction;

    SmapsReadResult<T> CalculateMemUsageFromSmaps<T>(Stream fileStream, int pid) where T : struct, ISmapsReaderResultAction;
}
