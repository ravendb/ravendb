using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Storage.Esent;

namespace Raven.StorageExporter
{
    public class EsentExportOperation : IDisposable
    {
        public EsentExportOperation(string dataDirPath, bool hasCompression, EncryptionConfiguration encryption)
        {
            var dbFullPath = Path.Combine(dataDirPath, "Data");            
            try
            {
                Api.JetCreateInstance(out instance, "instance");
                var ravenConfiguration = new RavenConfiguration();
                ravenConfiguration.DataDirectory = dataDirPath;
                ravenConfiguration.Storage.PreventSchemaUpdate = true;

                ITransactionalStorage storage;
                var success = StorageExporter.TryToCreateTransactionalStorage(ravenConfiguration, hasCompression, encryption, out storage);
                if (success == false)
                    ConsoleUtils.PrintErrorAndFail("Failed to create transactional storage");

                var configurator = new TransactionalStorageConfigurator(ravenConfiguration, (TransactionalStorage)storage);
                configurator.ConfigureInstance(instance, dataDirPath);
                storage.Dispose();
                Api.JetInit(ref instance);
                Api.JetBeginSession(instance, out sesid, null, null);           
                Api.JetAttachDatabase(sesid, dbFullPath, AttachDatabaseGrbit.None);
                Api.JetOpenDatabase(sesid, dbFullPath, null, out dbid, OpenDatabaseGrbit.None);
            }
            catch (Exception e)
            {
                ConsoleUtils.PrintErrorAndFail(e.Message, e.StackTrace);
            }
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
            if (tablesNames == null)
            {
                tablesNames = Api.GetTableNames(sesid, dbid);
            }
            return tablesNames;
        }

        private IEnumerable<string> tablesNames;
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
