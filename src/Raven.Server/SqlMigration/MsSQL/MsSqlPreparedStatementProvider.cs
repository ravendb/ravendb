using System;
using System.Data.SqlClient;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.MsSQL
{
    public class MsSqlPreparedStatementProvider<T> : IDataProvider<T>
    {
        private readonly SqlCommand _command;
        private readonly Func<DynamicJsonValue, object[]> _parametersProvider;
        private readonly Func<SqlDataReader, T> _extractor;
        private bool alreadyParametrized;
        
        public MsSqlPreparedStatementProvider(SqlConnection connection, string query, 
            Func<DynamicJsonValue, object[]> parametersProvider, Func<SqlDataReader, T> extractor)
        {
            _command = new SqlCommand(query, connection);
            _parametersProvider = parametersProvider;
            _extractor = extractor;
        }

        public T Provide(DynamicJsonValue specialColumns)
        {
            var parameters = _parametersProvider(specialColumns);
            if (alreadyParametrized)
            {
                for (var i = 0; i < parameters.Length; i++)
                {
                    _command.Parameters[i].Value = parameters[i];
                }
            } 
            else
            {
                for (var i = 0; i < parameters.Length; i++)
                {
                    _command.Parameters.AddWithValue("p" + i, parameters[i]);
                    
                    // set type explicitly to avoid: SqlCommand.Prepare method requires all parameters to have an explicitly set type.
                    _command.Parameters[i].DbType = _command.Parameters[i].DbType;
                }
                _command.Prepare();
                alreadyParametrized = true;
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
