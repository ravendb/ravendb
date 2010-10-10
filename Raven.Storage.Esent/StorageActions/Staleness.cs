using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Json;
using Raven.Database.Storage.StorageActions;

namespace Raven.Storage.Esent.StorageActions
{
    public partial class DocumentStorageActions : IStalenessStorageActions
    {
        public bool IsIndexStale(string name, DateTime? cutOff, string entityName)
        {
            Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
            Api.MakeKey(session, IndexesStats, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
            {
                return false;
            }
            if (IsStaleByEtag(entityName))
            {
                if (cutOff != null)
                {
                    var lastIndexedTimestamp =
                        Api.RetrieveColumnAsDateTime(session, IndexesStats,
                                                     tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"])
                            .Value;
                    if (cutOff.Value >= lastIndexedTimestamp)
                        return true;
                }
                else
                {
                    return true;
                }
            }

            Api.JetSetCurrentIndex(session, Tasks, "by_index");
            Api.MakeKey(session, Tasks, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
            {
                return false;
            }
            if (cutOff == null)
                return true;
            // we are at the first row for this index
            var addedAt = Api.RetrieveColumnAsDateTime(session, Tasks, tableColumnsCache.TasksColumns["added_at"]).Value;
            return cutOff.Value >= addedAt;
        }

        private bool IsStaleByEtag(string entityName)
        {
            var lastIndexedEtag = Api.RetrieveColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]);
            Api.JetSetCurrentIndex(session, Documents, "by_etag");
            if (!Api.TryMoveLast(session, Documents))
            {
                return false;
            }
            do
            {
                var lastEtag = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]);
                if (CompareArrays(lastEtag, lastIndexedEtag) <= 0)
                    break;

                if (entityName != null)
                {
                    var metadata =
                        Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).
                            ToJObject();
                    if (metadata.Value<string>("Raven-Entity-Name") != entityName)
                        continue;
                }
                return true;
            } while (Api.TryMovePrevious(session, Documents));
            return false;
        }

        private static int CompareArrays(byte[] docEtagBinary, byte[] indexEtagBinary)
        {
            for (int i = 0; i < docEtagBinary.Length; i++)
            {
                if (docEtagBinary[i].CompareTo(indexEtagBinary[i]) != 0)
                {
                    return docEtagBinary[i].CompareTo(indexEtagBinary[i]);
                }
            }
            return 0;
        }

    }
}