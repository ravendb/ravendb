using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Jurassic;
using Jurassic.Library;
using Raven.Client.Documents.Attachments;
using Raven.Client.ServerWide.ETL;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    internal class SqlDocumentTransformer : EtlTransformer<ToSqlItem, SqlTableWithRecords>
    {
        private const int DefaultSize = 50;

        private const string AttachmentMarker = "$attachment/";

        private readonly Transformation _transformation;
        private readonly SqlEtlConfiguration _config;
        private readonly Dictionary<string, SqlTableWithRecords> _tables;

        public SqlDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, SqlEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.SqlEtl))
        {
            _transformation = transformation;
            _config = config;
            _tables = new Dictionary<string, SqlTableWithRecords>(_config.SqlTables.Count);

            var tables = new string[config.SqlTables.Count];

            for (var i = 0; i < config.SqlTables.Count; i++)
            {
                tables[i] = config.SqlTables[i].TableName;
            }

            LoadToDestinations = tables;
        }

        protected override string[] LoadToDestinations { get; }

        public override void Initalize()
        {
            base.Initalize();
            if (SingleRun == null)
                return;
            SingleRun.ScriptEngine.SetGlobalFunction("varchar", (Func<string, int, ObjectInstance>)ToVarchar);
            SingleRun.ScriptEngine.SetGlobalFunction("nVarchar", (Func<string, int, ObjectInstance>)ToNVarchar);
            SingleRun.ScriptEngine.SetGlobalFunction(Transformation.LoadAttachment, (Func<string, string>)LoadAttachmentFunction);
        }

        protected override void LoadToFunction(string tableName, object cols)
        {
            if (tableName == null)
                ThrowLoadParameterIsMandatory(nameof(tableName));
            if (cols == null)
                ThrowLoadParameterIsMandatory(nameof(cols));

            BlittableJsonReaderObject result;
            using (var scriptResult = new ScriptRunnerResult(null, cols))
                result = scriptResult.Translate<BlittableJsonReaderObject>(Context);
            var columns = new List<SqlColumn>(result.Count);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < result.Count; i++)
            {
                result.GetPropertyByIndex(i, ref prop);

                var sqlColumn = new SqlColumn
                {
                    Id = prop.Name,
                    Value = prop.Value,
                    Type = prop.Token
                };


                if (_transformation.HasLoadAttachment && 
                    prop.Token == BlittableJsonToken.String && IsLoadAttachment(prop.Value as LazyStringValue, out var attachmentName))
                {
                    Stream attachmentStream;
                    using (Slice.From(Context.Allocator, Current.Document.ChangeVector, out var cv))
                    {
                        attachmentStream = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(
                                                         Context,
                                                         Current.DocumentId,
                                                         attachmentName,
                                                         AttachmentType.Document,
                                                         cv)
                                                     ?.Stream ?? Stream.Null;
                    }

                    sqlColumn.Type = 0;
                    sqlColumn.Value = attachmentStream;
                }

                columns.Add(sqlColumn);
            }

            GetOrAdd(tableName).Inserts.Add(new ToSqlItem(Current)
            {
                Columns = columns
            });
        }

        private static unsafe bool IsLoadAttachment(LazyStringValue value, out string attachmentName)
        {
            if (value.Length <= AttachmentMarker.Length)
            {
                attachmentName = null;
                return false;
            }

            var buffer = value.Buffer;

            if (*(long*)buffer != 7883660417928814884 || // $attachm
                *(int*)(buffer + 8) != 796159589) // ent/
            {
                attachmentName = null;
                return false;
            }

            attachmentName = value.Substring(AttachmentMarker.Length);

            return true;
        }

        private static string LoadAttachmentFunction(string attachmentName)
        {
            return $"{AttachmentMarker}{attachmentName}";
        }

        private SqlTableWithRecords GetOrAdd(string tableName)
        {
            if (_tables.TryGetValue(tableName, out SqlTableWithRecords table) == false)
            {
                _tables[tableName] =
                    table = new SqlTableWithRecords(_config.SqlTables.Find(x => x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)));
            }

            return table;
        }

        private ObjectInstance ToVarchar(string value, int size)
        {
            return SingleRun.ScriptEngine.Object.Construct(new
            {
                Type = DbType.AnsiString,
                Value = value,
                Size = size == 0 ? size : DefaultSize
            });

        }

        private ObjectInstance ToNVarchar(string value, int size)
        {
            return SingleRun.ScriptEngine.Object.Construct(new
            {
                Type = DbType.String,
                Value = value,
                Size = size == 0 ? size: DefaultSize
            });
        }

        public override IEnumerable<SqlTableWithRecords> GetTransformedResults()
        {
            return _tables.Values;
        }

        public override void Transform(ToSqlItem item)
        {
            if (item.IsDelete == false)
            {
                Current = item;

                SingleRun.Run(Context, "execute", new object[] { Current.Document }).Dispose();
            }

            // ReSharper disable once ForCanBeConvertedToForeach
            for (int i = 0; i < _config.SqlTables.Count; i++)
            {
                // delete all the rows that might already exist there

                var sqlTable = _config.SqlTables[i];

                if (sqlTable.InsertOnlyMode)
                    continue;

                GetOrAdd(sqlTable.TableName).Deletes.Add(item);
            }
        }
    }
}
