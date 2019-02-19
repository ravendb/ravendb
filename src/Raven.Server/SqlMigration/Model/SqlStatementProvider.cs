using System;
using System.Data.Common;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.MsSQL
{
    public class SqlStatementProvider<T> : IDataProvider<T> where T : class
    {
        private readonly DbCommand _command;
        private readonly Func<DynamicJsonValue, object[]> _parametersProvider;
        private readonly Func<DbDataReader, T> _extractor;
        
        public SqlStatementProvider(DbConnection connection, string query, 
            Func<DynamicJsonValue, object[]> parametersProvider, Func<DbDataReader, T> extractor)
        {
            _command = connection.CreateCommand();
            _command.CommandText = query;
            _parametersProvider = parametersProvider;
            _extractor = extractor;
        }

        public T Provide(DynamicJsonValue specialColumns)
        {
            var parameters = _parametersProvider(specialColumns);
            _command.Parameters.Clear();
            
            for (var i = 0; i < parameters.Length; i++)
            {
                // if any key is null when link doesn't exists
                if (parameters[i] == null)
                {
                    return null;
                }

                DbParameter parameter = _command.CreateParameter();
                parameter.ParameterName = "p" + i;
                parameter.Value = parameters[i];

                _command.Parameters.Add(parameter);
            }
            
            using (var reader = _command.ExecuteReader())
            {
                return _extractor(reader);
            }
        }

        public void Dispose()
        {
            _command.Dispose();
        }
    }
}
