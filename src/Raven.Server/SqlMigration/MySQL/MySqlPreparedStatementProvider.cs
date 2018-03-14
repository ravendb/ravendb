using System;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.MySQL
{
    public class MySqlPreparedStatementProvider<T> : IDataProvider<T>
    {
        private readonly MySqlCommand _command;
        private readonly Func<DynamicJsonValue, object[]> _parametersProvider;
        private readonly Func<MySqlDataReader, T> _extractor;
        private bool alreadyParametrized;
        
        public MySqlPreparedStatementProvider(MySqlConnection connection, string query, 
            Func<DynamicJsonValue, object[]> parametersProvider, Func<MySqlDataReader, T> extractor)
        {
            _command = new MySqlCommand(query, connection);
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
