using System;
using System.Data.SqlClient;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.MsSQL
{
    public class MsSqlStatementProvider<T> : IDataProvider<T> where T : class
    {
        private readonly SqlCommand _command;
        private readonly Func<DynamicJsonValue, object[]> _parametersProvider;
        private readonly Func<SqlDataReader, T> _extractor;
        
        public MsSqlStatementProvider(SqlConnection connection, string query, 
            Func<DynamicJsonValue, object[]> parametersProvider, Func<SqlDataReader, T> extractor)
        {
            _command = new SqlCommand(query, connection);
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
