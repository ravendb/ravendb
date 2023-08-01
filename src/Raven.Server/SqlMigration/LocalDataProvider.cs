using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration
{
    public sealed class LocalDataProvider<T> : IDataProvider<T>
    {
        private readonly Func<DynamicJsonValue, T> _provider;
        
        public LocalDataProvider(Func<DynamicJsonValue, T> provider)
        {
            _provider = provider;
        }
        
        public T Provide(DynamicJsonValue specialColumns)
        {
            return _provider(specialColumns);
        }

        public void Dispose()
        {
            // empty
        }
    }
}
