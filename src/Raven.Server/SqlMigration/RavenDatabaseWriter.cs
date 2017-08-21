using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
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
        private readonly DocumentsOperationContext _context;
        private List<string> _jsFunctions;
        private string _jsCode;
        private readonly SqlConnection _connection;
        private readonly bool _binaryToAttachment;
        private readonly Logger _logger;
        //private readonly int _reportPerDocument = 1000;
        //private int _documentsCount;

        public RavenDatabaseWriter(SqlDatabase sqlDatabase, DocumentDatabase documentDatabase, DocumentsOperationContext context, bool binaryToAttachment)
        {
            SqlDatabase = sqlDatabase;

            _documentDatabase = documentDatabase;
            _binaryToAttachment = binaryToAttachment;
            _context = context;
            _connection = SqlDatabase.Connection;
            //_documentsCount = 0;
            _jsFunctions = new List<string>();
            _jsCode = String.Empty;
            _logger = LoggingSource.Instance.GetLogger<RavenDatabaseWriter>("Sql migration");
        }

        public Dictionary<string, IDataReader> GetReadersOfEmbeddedTables(SqlTable table)
        {
            Dictionary<string, IDataReader> readers = new Dictionary<string, IDataReader>();

            foreach (var kvp in table.EmbeddedTables)
            {
                var childTable = SqlDatabase.GetTableByName(kvp.Value);
                var reference = childTable.GetReferenceColumnNameByTableName(table.Name);

                var embeddedQuery = $"select * from {SqlDatabase.TableQuote(childTable.Name)} order by '{reference.Key}'";

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
                if (table.IsEmbedded)
                    continue;

                // Execute queries parallelly
                var readers = GetReadersOfEmbeddedTables(table);

                /*string unsupportedStr = String.Empty;
                string removeUnsupportedStr = String.Empty;

                foreach (var item in table.UnsupportedColumns)
                {
                    unsupportedStr += $"CONVERT(varchar(256), {item}) as converted,";
                    if (removeUnsupportedStr == String.Empty)
                        removeUnsupportedStr = " ALTER TABLE #TempTable DROP COLUMN";
                    else
                        removeUnsupportedStr += ",";

                    removeUnsupportedStr += " " + item;
                }


                var query = "Select " + unsupportedStr + $"* INTO #TempTable from {SqlDatabase.TableQuote(table.Name)}" + removeUnsupportedStr + " SELECT * FROM #TempTable DROP TABLE #TempTable";
                Console.WriteLine(query);*/

                var query = $"Select * from {SqlDatabase.TableQuote(table.Name)}";

                using (var cmd = new SqlCommand(query, _connection))
                {
                    IDataReader reader;
                    try
                    {
                        reader = SqlHelper.ExecuteReader(cmd);
                    }
                    catch (Exception e)
                    {
                        var str = $"Cannot import table '{table.Name}' because it contains an unsupported data type.";
                        Console.WriteLine(e);
                        Console.WriteLine("\n\n\n\n\n\n");
                        if (_logger.IsInfoEnabled)
                            _logger.Info(str, e);
                        if(_connection.State == ConnectionState.Closed)
                            _connection.Open();
                        continue;
                    }
                    using (reader)
                    {
                        while (reader.Read())
                        {
                            Dictionary<string, byte[]> attachments = new Dictionary<string, byte[]>();

                            var document = GetDocumentFromSqlRow(table, reader, new List<string>(table.PrimaryKeys), false, ref attachments);

                            var id = GenerateIdFromSqlRow(reader, table);
                            document.SetCollection(table.Name);
                            document.SetEmbeddedTables(this, table, reader, readers, ref attachments);

                            var doc = _context.ReadObject(document, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            
                            await InsertDocument(doc, id, attachments);
                        }
                    }
                }
            }
            await EnqueueCommands();

        }

        private const int BatchSize = 1000;
        private int _count = 0;
        private readonly BatchRequestParser.CommandData[] _commands = new BatchRequestParser.CommandData[BatchSize];
        private readonly List<AttachmentHandler.MergedPutAttachmentCommand> _attachmentCommands = new List<AttachmentHandler.MergedPutAttachmentCommand>();
        private List<IDisposable> _toDispose = new List<IDisposable>();

        public async Task InsertDocument(BlittableJsonReaderObject document, string id, Dictionary<string, byte[]> attachments)
        {
            _commands[_count++] = new BatchRequestParser.CommandData
            {
                Type = CommandType.PUT,
                Document = document,
                Id = id
            };

            foreach (var attachment in attachments)
            {
                var streamsTempFile = _documentDatabase.DocumentsStorage.AttachmentsStorage.GetTempFile("put");
                var stream = streamsTempFile.StartNewStream();
                
                var ms = new MemoryStream(attachment.Value);
                var hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(_context, ms, stream, _documentDatabase.DatabaseShutdown);

                _attachmentCommands.Add(new AttachmentHandler.MergedPutAttachmentCommand
                {
                    Database = _documentDatabase,
                    DocumentId = id,
                    Name = $"{id}/{attachment.Key}",
                    Stream = stream,
                    Hash = hash,
                    ContentType = ""
                });
                _toDispose.Add(stream);
                _toDispose.Add(streamsTempFile);
             }

            if (_count % BatchSize == 0)
            {
                await EnqueueCommands();
                _count = 0;
            }
        }

        private async Task EnqueueCommands()
        {
            using (var command = new BatchHandler.MergedBatchCommand { Database = _documentDatabase })
            {
                command.ParsedCommands = new ArraySegment<BatchRequestParser.CommandData>(_commands, 0, _count);

                try
                {
                    await _documentDatabase.TxMerger.Enqueue(command);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }

                try
                {
                    foreach (var attachment in _attachmentCommands)
                        await _documentDatabase.TxMerger.Enqueue(attachment);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
                finally
                {
                    foreach (var disposable in _toDispose)
                    {
                        disposable.Dispose();
                    }
                    _toDispose = new List<IDisposable>();
                }

            }
        }

        private RavenDocument GetDocumentFromSqlRow(SqlTable table, IDataReader reader, List<string> primaryKeys, bool embedded, ref Dictionary<string, byte[]> attachments)
        {
            var document = new RavenDocument();
            
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var value = reader[i];
                var columnName = reader.GetName(i);

                bool inDoc = true;
                if (value is byte[])
                {
                    if (_binaryToAttachment && primaryKeys.Contains(columnName) == false)
                    {
                        attachments.Add(columnName, (byte[])value);
                        inDoc = false;
                    }
                    else
                        value = value.ToString();
                }


                if (inDoc && primaryKeys.Contains(columnName) == false || !embedded && table.PrimaryKeys.Count > 1)
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
                    this[key] = null;
                    return;
                }
                
                if (value is byte[])
                {
                    this[key] = System.Convert.ToBase64String((byte[]) value);
                    return;
                }

                if (value is Guid)
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

            public void SetEmbeddedTables(RavenDatabaseWriter writer, SqlTable parentTable, IDataReader parentReader, Dictionary<string, IDataReader> readers, ref Dictionary<string, byte[]> attachments)
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
                        RavenDocument innerDocument = writer.GetDocumentFromSqlRow(childTable, embeddedReader, parentTable.PrimaryKeys, true, ref attachments);

                        if (childTable.EmbeddedTables.Count > 0)
                            innerDocument.SetEmbeddedTables(writer, childTable, embeddedReader, writer.GetReadersOfEmbeddedTables(childTable), ref attachments);

                        Append(parentTableColumnName, innerDocument);
                    }
                    while (embeddedReader.Read() && value == embeddedReader[reference.Key].ToString());
                }
            }
        }
    }
}
