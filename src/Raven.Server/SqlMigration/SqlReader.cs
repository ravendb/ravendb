using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Raven.Server.SqlMigration
{
    public class SqlReader : IDisposable
    {
        private readonly IDbConnection _connection;
        private readonly SqlCommand _command;
        private readonly bool _oneTimeConnection;
        private IDataReader _reader;
        private bool _executed;
        public static int RowsRead;
        public bool Disposed => _reader.IsClosed;

        private static readonly List<SqlReader> AllReaders = new List<SqlReader>();

        public SqlReader(string connectionString, string query)
        {
            _oneTimeConnection = true;//oneTimeConnection means the connection can be closed after use.

            _connection = ConnectionFactory.OpenConnection(connectionString);

            _command = new SqlCommand(query, (SqlConnection)_connection);

            AllReaders.Add(this);
        }

        public SqlReader(IDbConnection connection, string query) 
        {
            _oneTimeConnection = false;

            _connection = connection;

            _command = new SqlCommand(query, (SqlConnection) _connection);

            AllReaders.Add(this);
        }

        public static void DisposeAll()
        {
            for (var i = 0; i < AllReaders.Count; i++)
                AllReaders[i].Dispose();
        }

        public SqlReader ExecuteReader()
        {
            _reader?.Dispose();

            try
            {
                _reader = _command.ExecuteReader();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to execute query: " + _command.CommandText, e);
            }

            _executed = true;
            return this;
        }

        public bool HasValue()
        {
            try
            {
                _reader.GetValue(0);
            }
            catch
            {
                return false;
            }

            return true;
        }

        public bool Read()
        {
            if (_executed == false)
                ExecuteReader();

            var read = _reader.Read();

            if (read)
                RowsRead++;

            return read;
        }

        public void Dispose()
        {
            if (_oneTimeConnection)
                _connection?.Dispose();

            _command?.Dispose();
            _reader?.Dispose();

            AllReaders.Remove(this);
        }

        public int FieldCount => _reader.FieldCount;

        public string GetName(int i) => _reader.GetName(i);

        public object this[int index] => _reader[index];

        public object this[string childColumn] => _reader[childColumn];

        public void AddParameter(string key, object value) => _command.Parameters.AddWithValue(key, value);

        public Type GetFieldType(int i) => _reader.GetFieldType(i);

        public string GetDataTypeName(int i) => _reader.GetDataTypeName(i);

        public void SetCommand(string query)
        {
            _command.CommandText = query;
            _executed = false;
        }
    }
}
