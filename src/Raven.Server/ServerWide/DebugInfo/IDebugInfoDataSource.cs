using System;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.DebugInfo
{
    public interface IDebugInfoDataSource : IDisposable
    {
        string FullPath { get; }
        Task<BlittableJsonReaderObject> GetData(JsonOperationContext context);
    }
}
