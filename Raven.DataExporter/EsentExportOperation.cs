using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.DataExporter
{
    public class TableExporter : IDisposable
    {
        public TableExporter(string dataDirPath)
        {
            var dbFullPath = Path.Combine(dataDirPath, "Data");
            Api.JetCreateInstance(out instance, "instance");
            var instanceParameters = new InstanceParameters(instance)
            {
                Recovery = false
            };
            Api.JetInit(ref instance);
            Api.JetBeginSession(instance, out sesid, null, null);
            try
            {
                Api.JetAttachDatabase(sesid, dbFullPath, AttachDatabaseGrbit.None);
                Api.JetOpenDatabase(sesid, dbFullPath, null, out dbid, OpenDatabaseGrbit.None);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            //Api.JetOpenDatabase(sesid, dbFullPath, null, out dbid, OpenDatabaseGrbit.None);
        }
        public void ExportTable(string tableName, string destinationPath)
        {
            var tables = GetTablesNames(dbid);
            if (!tables.Contains(tableName))
                throw new ArgumentException(string.Format("The given table name {0} is does not exsist in the database.\n",tableName) +
                                                          "available tables are:\n"+ string.Join(", ",tables));
            JET_TABLEID tableId;
            Api.JetOpenTable(sesid,dbid,tableName,null ,0 ,OpenTableGrbit.ReadOnly, out tableId);
            
            bool success = Api.TryMoveFirst(sesid, tableId);
            var columnInfo = Api.GetTableColumns(sesid, tableId);
            using(var writer = new CsvFileWriter(destinationPath))
            { 
                writer.WriteHeaders(columnInfo.Select(x => x.Name));                  
                if (!success) throw new Exception("Failed to move to the first record.");
                    while (success)
                    {
                        ExportSingleRecord(writer,columnInfo, tableId);
                        success = Api.TryMoveNext(sesid, tableId);
                }
            }

        }

        public void ExportDocuments(JsonTextWriter writer)
        {
            JET_TABLEID tableId;
            Api.JetOpenTable(sesid, dbid, "documents", null, 0, OpenTableGrbit.ReadOnly, out tableId);
            bool hasNext = Api.TryMoveFirst(sesid, tableId);
            var dataColumn = Api.GetTableColumnid(sesid, tableId, "data");
            var metadataColumn = Api.GetTableColumnid(sesid, tableId, "metadata");
            var idColumn = Api.GetTableColumnid(sesid, tableId, "key");
            var lastmodifyColumn = Api.GetTableColumnid(sesid, tableId, "last_modified");//GetDefaultRavenFormat
            var etagColumn = Api.GetTableColumnid(sesid, tableId, "etag");

            
            while (hasNext)
            {
                var doc = Api.RetrieveColumn(sesid, tableId, dataColumn).ToJObject();
                var metadata = Api.RetrieveColumn(sesid, tableId, metadataColumn).ToJObject();
                var id = Api.RetrieveColumnAsString(sesid, tableId, idColumn);
                var etag = Etag.Parse(Api.RetrieveColumn(sesid, tableId, etagColumn));
                var lastModify = DateTime.FromBinary(Api.RetrieveColumnAsInt64(sesid, tableId, lastmodifyColumn).Value);
                metadata.Add("@id", id);
                metadata.Add("@etag", etag.ToString());
                metadata.Add(Constants.LastModified, lastModify.GetDefaultRavenFormat(true));
                metadata.Add(Constants.RavenLastModified, lastModify.GetDefaultRavenFormat());
                doc.Add("@metadata", metadata);
                doc.WriteTo(writer);
                hasNext = Api.TryMoveNext(sesid, tableId);
            }
            Api.JetCloseTable(sesid,tableId);
        }

        public void ExportAttachments(JsonTextWriter writer)
        {
            JET_TABLEID tableId;
            Api.JetOpenTable(sesid, dbid, "files", null, 0, OpenTableGrbit.ReadOnly, out tableId);
            bool hasNext = Api.TryMoveFirst(sesid, tableId);
            var dataColumn = Api.GetTableColumnid(sesid, tableId, "data");
            var metadataColumn = Api.GetTableColumnid(sesid, tableId, "metadata");
            var etagColumn = Api.GetTableColumnid(sesid, tableId, "etag");
            var keyColumn = Api.GetTableColumnid(sesid, tableId, "name");
            //var columnInfo = Api.GetTableColumns(sesid, tableId);
            while (hasNext)
            {
                var data = Api.RetrieveColumn(sesid, tableId, dataColumn);
                //Api.RetrieveColumnAsString(sesid, tableId, column.Columnid)
                var metadata = RavenJObject.Parse(Api.RetrieveColumnAsString(sesid, tableId, metadataColumn));
                var etag = Etag.Parse(Api.RetrieveColumn(sesid, tableId, etagColumn));
                var key = Api.RetrieveColumnAsString(sesid, tableId, keyColumn);
                var obj = new RavenJObject();
                obj.Add("Data",data);
                obj.Add("Metadata", metadata);
                obj.Add("Key", key);
                obj.Add("Etag", etag.ToString());
                obj.WriteTo(writer);
                hasNext = Api.TryMoveNext(sesid, tableId);
            }
            Api.JetCloseTable(sesid, tableId);
        }

        public void ExportIdentities(JsonTextWriter writer)
        {
            JET_TABLEID tableId;
            Api.JetOpenTable(sesid, dbid, "identity_table", null, 0, OpenTableGrbit.ReadOnly, out tableId);
            bool hasNext = Api.TryMoveFirst(sesid, tableId);
            var keyColumn = Api.GetTableColumnid(sesid, tableId, "key");
            var valColumn = Api.GetTableColumnid(sesid, tableId, "val");
            while (hasNext)
            {
                var key = Api.RetrieveColumnAsString(sesid, tableId, keyColumn);
                if (FilterIdentify(key))
                {
                    hasNext = Api.TryMoveNext(sesid, tableId);
                    continue;
                }
                var value = Api.RetrieveColumnAsInt32(sesid, tableId, valColumn);
                new RavenJObject
						{
							{ "Key", key }, 
							{ "Value", value }
						}.WriteTo(writer);
                hasNext = Api.TryMoveNext(sesid, tableId);
            }
            Api.JetCloseTable(sesid, tableId);
        }

        private bool FilterIdentify(string key)
        {
            return filteredIdentities.Contains(key);
        }
        private static readonly List<string> filteredIdentities = new List<string>() { "Raven/Etag", "IndexId"}; 
        private void ExportSingleRecord(CsvFileWriter writer, IEnumerable<ColumnInfo> columnInfo, JET_TABLEID tableId)
        {
            foreach (var column in columnInfo)
            {
                var c = Api.RetrieveColumn(sesid, tableId, column.Columnid);
                switch (column.Coltyp)
                {
                    case JET_coltyp.Nil:
                    case JET_coltyp.Bit:
                    case JET_coltyp.UnsignedByte:
                    case JET_coltyp.Short:
                    case JET_coltyp.Currency:
                    case JET_coltyp.IEEESingle:
                    case JET_coltyp.IEEEDouble:
                        writer.WriteCsvColumnValue("unknown field");
                        break;
                    case JET_coltyp.Long:
                        writer.WriteCsvColumnValue(Api.RetrieveColumnAsInt32(sesid, tableId, column.Columnid).ToString());
                        break;
                    case JET_coltyp.DateTime:
                        writer.WriteCsvColumnValue(DateTime.FromBinary(Api.RetrieveColumnAsInt64(sesid, tableId, column.Columnid).Value).ToString());
                        break;
                    case JET_coltyp.Binary:
                        if (column.MaxLength == 8)
                        {
                            writer.WriteCsvColumnValue(DateTime.FromBinary(Api.RetrieveColumnAsInt64(sesid, tableId, column.Columnid).Value).ToString());
                        }
                        else if (column.MaxLength == 16)
                        {
                            var data = Api.RetrieveColumn(sesid, tableId, column.Columnid);
                            //etag is null
                            if (data == null)
                            {
                                writer.WriteCsvColumnValue("null");
                            }
                            else
                            {
                                writer.WriteCsvColumnValue(Etag.Parse(data));
                            } 
                        }                        
                        else if (column.MaxLength == 20)
                        {
                            var sha1Hash = BitConverter.ToString(Api.RetrieveColumn(sesid, tableId, column.Columnid));
                            writer.WriteCsvColumnValue(sha1Hash);
                        }
                        else
                        {
                            writer.WriteCsvColumnValue("unknown binary field");
                        }
                        break;
                    case JET_coltyp.Text:
                        writer.WriteCsvColumnValue(Api.RetrieveColumnAsString(sesid, tableId, column.Columnid));
                        break;
                    case JET_coltyp.LongBinary:

                        writer.WriteCsvColumnValue(Api.RetrieveColumn(sesid, tableId, column.Columnid).ToJObject().ToString(),escape:true);
                        break;
                    case JET_coltyp.LongText:
                        writer.WriteCsvColumnValue(Api.RetrieveColumnAsString(sesid, tableId, column.Columnid));
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }                
            }
        }
        private IEnumerable<string> GetTablesNames(JET_DBID dbid)
        {
            if (!databasesToTablesNames.ContainsKey(dbid))
            {
                databasesToTablesNames[dbid] = Api.GetTableNames(sesid, dbid);
            }
            return databasesToTablesNames[dbid];
        }
        private Dictionary<JET_DBID, IEnumerable<string>> databasesToTablesNames = new Dictionary<JET_DBID, IEnumerable<string>>();
        private JET_INSTANCE instance;
        private JET_SESID sesid;
        private JET_DBID dbid;
        public void Dispose()
        {
            Api.JetCloseDatabase(sesid,dbid,CloseDatabaseGrbit.None);
            Api.JetEndSession(sesid,EndSessionGrbit.None);
            Api.JetTerm(instance);
        }
    }
}
