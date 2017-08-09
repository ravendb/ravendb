using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Jint;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using CommandType = Raven.Client.Documents.Commands.Batches.CommandType;

namespace Raven.Server.SqlMigration
{
    public delegate void TableWrittenEventHandler(string tableName);
    public delegate void DocumentsEventHandler(int documentsCount);

    class RavenDatabaseWriter
    {
        public readonly SqlDatabase SqlDatabase;
        //public event TableWrittenEventHandler OnTableWritten;
        //public event DocumentsEventHandler OnDocumentsWritten;

        private readonly DocumentDatabase _documentDatabase;
        private readonly JsonOperationContext _context;
        private List<string> _jsFunctions;
        private string _jsCode;
        private readonly SqlConnection _connection;
        //private readonly int _reportPerDocument = 1000;
        //private int _documentsCount;

        public RavenDatabaseWriter(SqlDatabase sqlDatabase, DocumentDatabase documentDatabase, JsonOperationContext context)
        {
            SqlDatabase = sqlDatabase;
            _documentDatabase = documentDatabase;

            _context = context;

            _connection = SqlDatabase.Connection;
            //_documentsCount = 0;
            _jsFunctions = new List<string>();
            _jsCode = string.Empty;
        }

        public Dictionary<string, IDataReader> GetReadersOfEmbeddedTables(SqlTable table)
        {
            Dictionary<string, IDataReader> readers = new Dictionary<string, IDataReader>();

            foreach (var kvp in table.EmbeddedTables)
            {
                var childTable = SqlDatabase.GetTableByName(kvp.Value);
                var reference = childTable.GetReferenceColumnNameByTableName(table.Name);

                var embeddedQuery = $"select * from {SqlHelper.TableQuote(childTable.Name)} order by '{reference.Key}'";

                var con = new ConnectionFactory(_connection.ConnectionString).OpenConnection();
                var cmd = new SqlCommand(embeddedQuery, (SqlConnection)con);

                readers.Add(kvp.Value, SqlHelper.ExecuteReader(cmd));
            }

            return readers;
        }

        public void AddJSModification(string filePath, string functionName)
        {
            string jsCode = File.ReadAllText(filePath);

            _jsCode += jsCode;
            _jsFunctions.Add(functionName);
        }

        public async Task WriteDatabase()
        {
            foreach (var table in SqlDatabase.Tables)
            {
                Console.WriteLine(table.Name);
                if (table.IsEmbedded)
                    continue;

                // Execute queries parallelly
                var readers = GetReadersOfEmbeddedTables(table);

                var query = $"Select * from {SqlHelper.TableQuote(table.Name)}";

                using (var cmd = new SqlCommand(query, _connection))
                {
                    IDataReader reader;
                    try
                    {
                        reader = SqlHelper.ExecuteReader(cmd);
                    }
                    catch (Exception)
                    {
                        _connection.Open();
                        continue;
                    }
                    using (reader)
                    {
                        while (reader.Read())
                        {
                            var document = GetDocumentFromSqlRow(table, reader, new List<string>(table.PrimaryKeys), false);

                            var id = GenerateIdFromSqlRow(reader, table);
                            document.SetCollection(table.Name);
                            document.SetEmbeddedTables(this, table, reader, readers);


                            var doc = _context.ReadObject(document, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            /*var doc = EntityToBlittable.ConvertEntityToBlittable(document, DocumentConventions.Default, _context, new DocumentInfo
                            {
                                Collection = table.Name
                            });*/
                            
                            await InsertDocument(doc, id);
                            
                        }
                    }
                }
            }
            await InsertDocument(null, null, true);

        }

        public async Task InsertDocument1(BlittableJsonReaderObject document, string id)
        {
            using (var command = new BatchHandler.MergedBatchCommand
            {
                Database = _documentDatabase
            })
            {

                command.ParsedCommands = new ArraySegment<BatchRequestParser.CommandData>(new[]{new BatchRequestParser.CommandData()
                {
                    Type =  CommandType.PUT,
                    Document = document,
                    Id = id
                }});


                try
                {
                    await _documentDatabase.TxMerger.Enqueue(command);
                }
                catch (ConcurrencyException)
                {/*
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
                    throw;*/
                }
            }
        }

        private int indexInCommands = 0;
        private BatchRequestParser.CommandData[] Commands = new BatchRequestParser.CommandData[1000];
        public async Task InsertDocument(BlittableJsonReaderObject document, string id, bool force = false)
        {

            if (document != null)
                Commands[indexInCommands] = new BatchRequestParser.CommandData()
                {
                    Type = CommandType.PUT,
                    Document = document,
                    Id = id
                };

            if (indexInCommands > 0 && indexInCommands % 999 == 0 || indexInCommands > 0 && force)
            {
                using (var command = new BatchHandler.MergedBatchCommand
                {
                    Database = _documentDatabase
                })
                {

                    command.ParsedCommands = new ArraySegment<BatchRequestParser.CommandData>(Commands, 0, indexInCommands+1);

                    try
                    {

                        await _documentDatabase.TxMerger.Enqueue(command);

                    }
                    catch (ConcurrencyException e)
                    {
                        // todo: decide what to do with exceptions
                        Console.WriteLine(e);
                        /*
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
                        throw;*/
                    }
                    finally
                    {
                        
                    }
                }
                indexInCommands = 0;

            }
            else
            {
                indexInCommands++;
            }


        }

        private static RavenDocument GetDocumentFromSqlRow(SqlTable table, IDataReader reader, List<string> primaryKeys, bool embedded)
        {
            var document = new RavenDocument();
            
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var value = reader[i];
                var columnName = reader.GetName(i);

                if (primaryKeys.Contains(columnName) == false || (!embedded && table.PrimaryKeys.Count > 1))
                {
                    if (value == DBNull.Value)
                        value = null;

                    else if (table.References.TryGetValue(columnName, out var reference))
                        value = $"{reference.Item1}/{value}";

                    document.Set(columnName, value);
                }
            }
            return document;
        }

        private string GenerateIdFromSqlRow(IDataReader reader, SqlTable table)
        {
            var Id = table.Name;

            foreach (string pkName in table.PrimaryKeys)
                Id += $"/{reader[pkName]}";

            return Id;
        }

        class RavenDocument : DynamicJsonValue
        {
            public void Set(string key, object value)
            {

                if (value == null)
                {
                    this[key] = value;
                    return;
                }

                var val1 = value as byte[];
                if (val1 != null)
                {
                    this[key] = System.Convert.ToBase64String(val1);
                    return;
                }
                
                if (Guid.TryParse(value.ToString(), out _))
                {
                    this[key] = value.ToString();
                    return;
                }
                
                this[key] = value;
            }

            private void Append(string key, RavenDocument ravenDocument)
            {
                if (this[key] == null)
                    this[key] = new List<RavenDocument>();

                List<RavenDocument> lst = (List<RavenDocument>) this[key];
                lst.Add(ravenDocument);
            }

            public void SetCollection(string collectionName)
            {
                this["@metadata"] = new RavenDocument
                    {
                        ["@collection"] = collectionName
                    };
            }

            public void SetEmbeddedTables(RavenDatabaseWriter writer, SqlTable parentTable, IDataReader parentReader, Dictionary<string, IDataReader> readers)
            {
                foreach (var item in parentTable.EmbeddedTables)
                {
                    var childTable = writer.SqlDatabase.GetTableByName(item.Value);

                    var parentTableColumnName = item.Key;

                    IDataReader embeddedReader = readers[item.Value];

                    var reference = childTable.GetReferenceColumnNameByTableName(parentTable.Name);

                    string value = parentReader[reference.Value.Item2].ToString();

                    bool finished = false;

                    try
                    {
                        var val = embeddedReader[reference.Key];
                    }
                    catch (Exception)
                    {
                        embeddedReader.Read();

                        while (true)
                        {
                            var val = embeddedReader[reference.Key].ToString();
                            if (value == val)
                                break;

                            if (!embeddedReader.Read())
                            {
                                finished = true;
                                break;
                            }
                        }
                    }

                    if (finished)
                        continue;

                    do
                    {
                        RavenDocument innerDocument = GetDocumentFromSqlRow(childTable, embeddedReader, parentTable.PrimaryKeys, true);

                        if (childTable.EmbeddedTables.Count > 0)
                            innerDocument.SetEmbeddedTables(writer, childTable, embeddedReader, writer.GetReadersOfEmbeddedTables(childTable));


                        Append(parentTableColumnName, innerDocument);
                    }
                    while (embeddedReader.Read() && value == embeddedReader[reference.Key].ToString());
                }
            }
        }
    }
}
