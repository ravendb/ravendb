using System;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.MySQL
{
    public class MySqlStatementProvider<T> : IDataProvider<T> where T : class
    {
        private readonly MySqlCommand _command;
        private readonly Func<DynamicJsonValue, object[]> _parametersProvider;
        private readonly Func<MySqlDataReader, T> _extractor;

        public MySqlStatementProvider(MySqlConnection connection, string query,
            Func<DynamicJsonValue, object[]> parametersProvider, Func<MySqlDataReader, T> extractor)
        {
            _command = new MySqlCommand(query, connection);
            _parametersProvider = parametersProvider;
            _extractor = extractor;
        }

        public T Provide(DynamicJsonValue specialColumns)
        {
            _command.Parameters.Clear();
            var parameters = _parametersProvider(specialColumns);
            for (var i = 0; i < parameters.Length; i++)
            {
                // if any key is null when link doesn't exists
                if (parameters[i] == null)
                {
                    return null;
                }
                _command.Parameters.AddWithValue("p" + i, parameters[i]);
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
