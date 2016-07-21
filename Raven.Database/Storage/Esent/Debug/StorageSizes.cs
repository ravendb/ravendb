// -----------------------------------------------------------------------
//  <copyright file="StorageSizes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Util;
using Raven.Storage.Esent;
using Raven.Abstractions.Data;

namespace Raven.Database.Storage.Esent.Debug
{
    public static class StorageSizes
    {
        public static List<string> ReportOn(string path)
        {
            using (var transactionalStorage = new TransactionalStorage(new RavenConfiguration
            {
                DataDirectory = path,
                Settings =
                {
                    {Constants.Esent.LogFileSize, "1"}
                }
            }, () => { }, () => { }, () => { }, () => { }))
            {
                transactionalStorage.Initialize(new DummyUuidGenerator(), new OrderedPartCollection<AbstractDocumentCodec>());

                return ReportOn(transactionalStorage, status => { }, CancellationToken.None);
            }
        }

        public static List<string> ReportOn(TransactionalStorage transactionalStorage, Action<string> progress, CancellationToken token)
        {
            var list = new List<string>();
            transactionalStorage.Batch(accessor =>
            {
                var session = ((StorageActionsAccessor)accessor).Inner.Session;
                var jetDbid = ((StorageActionsAccessor)accessor).Inner.Dbid;

                token.ThrowIfCancellationRequested();

                var dictionary = GetSizes(session, jetDbid, progress, token);
                list.AddRange(dictionary.OrderByDescending(x => x.Item2).Select(l => l.Item1));
            });
            return list;
        }

        private static IEnumerable<Tuple<string, long>> GetSizes(Session session, JET_DBID db, Action<string> progress, CancellationToken token)
        {
            progress("Calculating total db size");

            int dbPages;
            Api.JetGetDatabaseInfo(session, db, out dbPages, JET_DbInfo.Filesize);

            var dbTotalSize = (long)dbPages * SystemParameters.DatabasePageSize;
            yield return Tuple.Create("Total db size: " + SizeHelper.Humane(dbTotalSize), dbTotalSize);

            token.ThrowIfCancellationRequested();

            foreach (var tableName in Api.GetTableNames(session, db))
            {
                using (var tbl = new Table(session, db, tableName, OpenTableGrbit.None))
                {
                    token.ThrowIfCancellationRequested();
                    progress("Computing stats for table: " + tableName);
                    Api.JetComputeStats(session, tbl);

                    JET_OBJECTINFO result;
                    Api.JetGetTableInfo(session, tbl, out result, JET_TblInfo.Default);
                    var sb = new StringBuilder(tableName).AppendLine();
                    var usedSize = (long)result.cPage * SystemParameters.DatabasePageSize;
                    int ownedPages;
                    Api.JetGetTableInfo(session, tbl, out ownedPages, JET_TblInfo.SpaceOwned);

                    sb.Append("\tOwned Size: ")
                      .Append(SizeHelper.Humane((long)ownedPages * SystemParameters.DatabasePageSize))
                      .AppendLine();


                    sb.Append("\tUsed Size: ")
                      .Append(SizeHelper.Humane(usedSize))
                      .AppendLine();


                    sb.Append("\tRecords: ").AppendFormat("{0:#,#;;0}", result.cRecord).AppendLine();
                    sb.Append("\tIndexes:").AppendLine();

                    foreach (var index in Api.GetTableIndexes(session, tbl))
                    {
                        token.ThrowIfCancellationRequested();

                        sb.Append("\t\t")
                          .Append(index.Name)
                          .Append(": ")
                          .Append(SizeHelper.Humane((long)index.Pages * (SystemParameters.DatabasePageSize)))
                          .AppendLine();
                    }
                    yield return Tuple.Create(sb.ToString(), (long)ownedPages * SystemParameters.DatabasePageSize);
                }
            }
        }
    }
}
