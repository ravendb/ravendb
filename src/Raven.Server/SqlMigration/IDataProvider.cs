using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration
{
    public interface IDataProvider<out T> : IDisposable
    {
        T Provide(DynamicJsonValue specialColumns);
    }
    
}
