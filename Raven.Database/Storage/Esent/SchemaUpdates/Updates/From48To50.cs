// -----------------------------------------------------------------------
//  <copyright file="From46To47.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Microsoft.Isam.Esent.Interop;

using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Imports.Newtonsoft.Json;
using Raven.Storage.Esent;
using Raven.Storage.Esent.SchemaUpdates;

namespace Raven.Database.Storage.Esent.SchemaUpdates.Updates
{
    public class From48To50 : ISchemaUpdate
    {
        private InMemoryRavenConfiguration configuration;

        public string FromSchemaVersion { get { return "4.8"; } }

        public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void Update(Session session, JET_DBID dbid, Action<string> output)
        {
            using (var tbl = new Table(session, dbid, "documents", OpenTableGrbit.None))
            {
                Api.JetDeleteIndex(session, tbl, "by_key");
                Api.JetCreateIndex2(session, tbl, new[]
                {
                    new JET_INDEXCREATE
                    {
                        szIndexName = "by_key",
                        cbKey = 6,
                        cbKeyMost = SystemParameters.KeyMost,
                        cbVarSegMac = SystemParameters.KeyMost,
                        szKey = "+key\0\0",
                        grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                    }
                }, 1);
            }

            // So first we allocate ids and crap
            // and write that to disk in a safe manner
            // I might want to look at keeping a list of written files to delete if it all goes tits up at any point
            var filesToDelete = new List<string>();
            var nameToIds = new Dictionary<string, int>();
            var indexDefPath = Path.Combine(configuration.DataDirectory, "IndexDefinitions");

            var indexDefinitions = Directory.GetFiles(indexDefPath, "*.index")
                                        .Select(x => { filesToDelete.Add(x); return x; })
                                        .Select(index => JsonConvert.DeserializeObject<IndexDefinition>(File.ReadAllText(index), Default.Converters))
                                        .ToList();

            indexDefinitions.ForEach(x => x.MaxIndexOutputsPerDocument = x.MaxIndexOutputsPerDocument ?? (16 * 1024));

            var transformDefinitions = Directory.GetFiles(indexDefPath, "*.transform")
                                        .Select(x => { filesToDelete.Add(x); return x; })
                                        .Select(index => JsonConvert.DeserializeObject<TransformerDefinition>(File.ReadAllText(index), Default.Converters))
                                        .ToArray();

            int maxIndexId = 0;

            for (var i = 0; i < indexDefinitions.Count; i++)
            {
                var definition = indexDefinitions[i];
                definition.IndexId = i;
                nameToIds[definition.Name] = definition.IndexId;
                var path = Path.Combine(indexDefPath, definition.IndexId + ".index");

                // TODO: This can fail, rollback
                File.WriteAllText(path, JsonConvert.SerializeObject(definition, Formatting.Indented, Default.Converters));

                var indexDirectory = FixupIndexName(definition.Name, configuration.IndexStoragePath);
                var oldStorageDirectory = Path.Combine(configuration.IndexStoragePath, MonoHttpUtility.UrlEncode(indexDirectory));
                var newStorageDirectory = Path.Combine(configuration.IndexStoragePath, definition.IndexId.ToString());

                if (Directory.Exists(oldStorageDirectory)) // in-memory index
                    Directory.Move(oldStorageDirectory, newStorageDirectory);

                maxIndexId = i;
            }

            for (var i = 0; i < transformDefinitions.Length; i++)
            {
                var definition = transformDefinitions[i];
                definition.TransfomerId = maxIndexId = indexDefinitions.Count + i;
                nameToIds[definition.Name] = definition.TransfomerId;
                var path = Path.Combine(indexDefPath, definition.TransfomerId + ".transform");

                // TODO: This can file, rollback
                File.WriteAllText(path, JsonConvert.SerializeObject(definition, Formatting.Indented, Default.Converters));
            }



            var tablesAndColumns = new[]
            {
                new {table = "scheduled_reductions", column = "view"},
                new {table = "mapped_results", column = "view"},
                new {table = "reduce_results", column = "view"},
                new {table = "reduce_keys_counts", column = "view"},
                new {table = "reduce_keys_status", column = "view"},
                new {table = "indexed_documents_references", column = "view"},
                new {table = "tasks", column = "for_index"},
                new {table = "indexes_stats", column = "key"},
                new {table = "indexes_etag", column = "key"},
                new {table = "indexes_stats_reduce", column = "key"}
            };

            foreach (var item in tablesAndColumns)
            {
                var newTable = item.table + "_new";
                JET_TABLEID newTableId;
                try
                {
                    Api.JetDeleteTable(session, dbid, newTable);
                }
                catch (Exception)
                {
                    //if there is no such table - then it is not important
                    //this is a precaution against partially failed upgrade process
                }

                using (var sr = new Table(session, dbid, item.table, OpenTableGrbit.None))
                {
                    Api.JetCreateTable(session, dbid, newTable, 1, 80, out newTableId);
                    var existingColumns = Api.GetTableColumns(session, sr).ToList();
                    var existingIndexes = Api.GetTableIndexes(session, sr).ToList();

                    foreach (var column in existingColumns)
                    {
                        JET_COLUMNDEF columnDef = null;
                        Api.JetGetColumnInfo(session, dbid, item.table, column.Name, out columnDef);
                        JET_COLUMNID newColumndId;

                        if (column.Name == item.column)
                        {
                            Api.JetAddColumn(session, newTableId, item.column, new JET_COLUMNDEF
                            {
                                coltyp = JET_coltyp.Long,
                                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
                            }, null, 0, out newColumndId);
                        }
                        else
                        {
                            var defaultValue = column.DefaultValue == null ? null : column.DefaultValue.ToArray();
                            var defaultValueLength = defaultValue == null ? 0 : defaultValue.Length;
                            {
                                Api.JetAddColumn(session, newTableId, column.Name, columnDef, defaultValue,
                                    defaultValueLength, out newColumndId);
                            }
                        }
                    }

                    foreach (var index in existingIndexes)
                    {
                        var indexDesc = String.Join("\0", index.IndexSegments.Select(x => "+" + x.ColumnName)) +
                                        "\0\0";
                        SchemaCreator.CreateIndexes(session, newTableId, new JET_INDEXCREATE()
                        {
                            szIndexName = index.Name,
                            szKey = indexDesc,
                            grbit = index.Grbit
                        });
                    }

                    var rows = 0;
                    using (var destTable = new Table(session, dbid, newTable, OpenTableGrbit.None))
                    {
                        Api.MoveBeforeFirst(session, sr);
                        Api.MoveBeforeFirst(session, destTable);

                        while (Api.TryMoveNext(session, sr))
                        {
                            using (var insert = new Update(session, destTable, JET_prep.Insert))
                            {
                                bool save = true;
                                foreach (var column in existingColumns)
                                {
                                    var destcolumn = Api.GetTableColumnid(session, destTable, column.Name);
                                    if (column.Name == item.column)
                                    {
                                        var viewName = Api.RetrieveColumnAsString(session, sr, column.Columnid,
                                            Encoding.Unicode);
                                        int value;
                                        if (nameToIds.TryGetValue(viewName, out value) == false)
                                        {
                                            insert.Cancel();
                                            save = false;
                                            break;
                                        }
                                        Api.SetColumn(session, destTable, destcolumn, value);
                                    }
                                    else if ((column.Grbit & ColumndefGrbit.ColumnAutoincrement) == ColumndefGrbit.None)
                                    {
                                        var value = Api.RetrieveColumn(session, sr, column.Columnid);
                                        Api.SetColumn(session, destTable, destcolumn, value);
                                    }
                                }
                                if (save)
                                    insert.Save();
                            }

                            if (rows++ % 10000 == 0)
                            {
                                output("Processed " + (rows) + " rows in " + item.table);
                                Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush);
                                Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
                            }
                        }
                    }
                    output("Processed " + (rows - 1) + " rows in " + item.table + ", and done with this table");
                }
                Api.JetCommitTransaction(session, CommitTransactionGrbit.None);
                Api.JetDeleteTable(session, dbid, item.table);
                Api.JetRenameTable(session, dbid, newTable, item.table);
                Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
            }

            filesToDelete.ForEach(File.Delete);
            UpdateLastIdentityForIndexes(session, dbid, maxIndexId + 1);
            SchemaCreator.UpdateVersion(session, dbid, "5.0");
        }

        private void UpdateLastIdentityForIndexes(Session session, JET_DBID dbid, int lastIdentity)
        {
            using (var sr = new Table(session, dbid, "identity_table", OpenTableGrbit.None))
            {
                Api.JetSetCurrentIndex(session, sr, "by_key");
                Api.MakeKey(session, sr, "IndexId", Encoding.Unicode, MakeKeyGrbit.NewKey);
                using (var update = new Update(session, sr, Api.TrySeek(session, sr, SeekGrbit.SeekEQ) ? JET_prep.Replace : JET_prep.Insert))
                {
                    var keyId = Api.GetTableColumnid(session, sr, "key");
                    var valId = Api.GetTableColumnid(session, sr, "val");
                    Api.SetColumn(session, sr, keyId, "IndexId", Encoding.Unicode);
                    Api.SetColumn(session, sr, valId, lastIdentity);
                    update.Save();
                }
            }
        }

        public static string FixupIndexName(string index, string path)
        {
            if (index.EndsWith("=")) //allready encoded
                return index;
            index = index.Trim();
            string prefix = null;
            if (index.StartsWith("Temp/", StringComparison.OrdinalIgnoreCase) || index.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase))
            {
                prefix = index.Substring(0, 5);
            }
            if (path.Length + index.Length > 230 ||
                Encoding.Unicode.GetByteCount(index) >= 255)
            {
                var bytes = Encryptor.Current.Hash.Compute16(Encoding.UTF8.GetBytes(index));
                var result = prefix + Convert.ToBase64String(bytes);

                if (path.Length + result.Length > 230)
                    throw new InvalidDataException("index name with the given path is too long even after encoding: " + index);

                return result;
            }
            return index;
        }
    }
}
