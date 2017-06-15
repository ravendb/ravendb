using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.DebugInfo
{
    public class DatabaseEndpointDebugInfoDataSource : IDebugInfoDataSource
    {
        private readonly string _databaseName;
        private readonly string _url;
        
        public string FullPath { get; }

        public DatabaseEndpointDebugInfoDataSource(string databaseName, string url, string fullPath)
        {
            _databaseName = databaseName;
            _url = url;
            FullPath = fullPath;
        }

        public async Task<BlittableJsonReaderObject> GetData(JsonOperationContext context)
        {
            await Task.Delay(100);
            throw new NotImplementedException();
        }

        public void Dispose()
        {            
        }
    }
}
