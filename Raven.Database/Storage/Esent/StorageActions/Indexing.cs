//-----------------------------------------------------------------------
// <copyright file="Indexing.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Storage;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IIndexingStorageActions
	{
		private bool SetCurrentIndexStatsToImpl(string index)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
				throw new IndexDoesNotExistsException("There is no index named: " + index);

			// this is optional
			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.MakeKey(session, IndexesStatsReduce, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			return Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ);
		}

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.JetSetCurrentIndex(session, IndexesEtags, "by_key");

			Api.MoveBeforeFirst(session, IndexesStats);
			while (Api.TryMoveNext(session, IndexesStats))
			{
				yield return GetIndexStats();
			}
		}

		public IndexStats GetIndexStats(string index)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.JetSetCurrentIndex(session, IndexesEtags, "by_key");

			Api.MakeKey(session, IndexesStats, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
				return null;

			return GetIndexStats();
		}

		private IndexStats GetIndexStats()
		{
			var indexName = Api.RetrieveColumnAsString(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["key"]);
			Api.MakeKey(session, IndexesStatsReduce, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var hasReduce = Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ);

			Api.MakeKey(session, IndexesEtags, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.TrySeek(session, IndexesEtags, SeekGrbit.SeekEQ);

			var lastIndexedTimestamp = Api.RetrieveColumnAsInt64(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"]).Value;
			return new IndexStats
			{
				Name = indexName,
				TouchCount = Api.RetrieveColumnAsInt32(session, IndexesEtags, tableColumnsCache.IndexesEtagsColumns["touches"]).Value,
				IndexingAttempts =
					Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"]).Value,
				IndexingSuccesses =
					Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"]).Value,
				IndexingErrors =
					Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"]).Value,
				LastIndexedEtag =
					Api.RetrieveColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]).
						TransfromToGuidWithProperSorting(),
				LastIndexedTimestamp = DateTime.FromBinary(lastIndexedTimestamp),
				ReduceIndexingAttempts =
					hasReduce == false
						? null
						: Api.RetrieveColumnAsInt32(session, IndexesStatsReduce,
													tableColumnsCache.IndexesStatsReduceColumns["reduce_attempts"]),
				ReduceIndexingSuccesses = hasReduce == false
											  ? null
											  : Api.RetrieveColumnAsInt32(session, IndexesStatsReduce,
																		  tableColumnsCache.IndexesStatsReduceColumns["reduce_successes"]),
				ReduceIndexingErrors = hasReduce == false
										   ? null
										   : Api.RetrieveColumnAsInt32(session, IndexesStatsReduce,
																	   tableColumnsCache.IndexesStatsReduceColumns["reduce_errors"]),
				LastReducedEtag = hasReduce == false ? (Guid?)null : GetLastReduceIndexWithPotentialNull(),
				LastReducedTimestamp = hasReduce == false ? (DateTime?)null : GetLastReducedTimestampWithPotentialNull(),
			};
		}

		private DateTime GetLastReducedTimestampWithPotentialNull()
		{
			var binary = Api.RetrieveColumnAsInt64(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"]);
			if (binary == null)
				return DateTime.MinValue;
			return DateTime.FromBinary(binary.Value);
		}

		private Guid GetLastReduceIndexWithPotentialNull()
		{
			var bytes = Api.RetrieveColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_etag"]);
			if (bytes == null)
				return Guid.Empty;
			return bytes.TransfromToGuidWithProperSorting();
		}

		public void AddIndex(string name, bool createMapReduce)
		{
			using (var update = new Update(session, IndexesStats, JET_prep.Insert))
			{
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["key"], name, Encoding.Unicode);
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_etag"], Guid.Empty.TransformToValueForEsentSorting());
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"], DateTime.MinValue.ToBinary());
				update.Save();
			}


			using (var update = new Update(session, IndexesEtags, JET_prep.Insert))
			{
				Api.SetColumn(session, IndexesEtags, tableColumnsCache.IndexesEtagsColumns["key"], name, Encoding.Unicode);
				update.Save();
			}

			if (createMapReduce == false)
				return;

			using (var update = new Update(session, IndexesStatsReduce, JET_prep.Insert))
			{
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["key"], name, Encoding.Unicode);
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_etag"], Guid.Empty.TransformToValueForEsentSorting());
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"], DateTime.MinValue.ToBinary());
				update.Save();
			}
		}

		public void DeleteIndex(string name)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ))
			{
				Api.JetDelete(session, IndexesStats);
			}

			Api.JetSetCurrentIndex(session, IndexesEtags, "by_key");
			Api.MakeKey(session, IndexesEtags, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesEtags, SeekGrbit.SeekEQ))
			{
				Api.JetDelete(session, IndexesEtags);
			}

			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.MakeKey(session, IndexesStatsReduce, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ))
			{
				Api.JetDelete(session, IndexesStatsReduce);
			}

			foreach (var op in new[]
			{
				new { Table = MappedResults, Index = "by_view_and_doc_key" },
				new { Table = ScheduledReductions, Index = "by_view_level_bucket_and_hashed_reduce_key" },
				new { Table = ReducedResults, Index = "by_view_level_hashed_reduce_key_and_bucket" },
				new { Table = ReduceKeysCounts, Index = "by_view_and_hashed_reduce_key" },
				new { Table = ReduceKeysStatus, Index = "by_view_and_hashed_reduce_key" },
				new { Table = IndexedDocumentsReferences, Index = "by_view_and_key" },
			})
			{
				Api.JetSetCurrentIndex(session, op.Table, op.Index);
				Api.MakeKey(session, op.Table, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
				if (!Api.TrySeek(session, op.Table, SeekGrbit.SeekGE))
					continue;
				var columnids = Api.GetColumnDictionary(session, op.Table);
				var count = 0;
				do
				{
					var indexNameFromDb = Api.RetrieveColumnAsString(session, op.Table, columnids["view"]);
					if (string.Equals(name, indexNameFromDb, StringComparison.InvariantCultureIgnoreCase) == false)
						break;
					if (count++ > 10000)
					{
						PulseTransaction();
						count = 0;
					}

					Api.JetDelete(session, op.Table);
				} while (Api.TryMoveNext(session, op.Table));
			}

		}

		public IndexFailureInformation GetFailureRate(string index)
		{
			var hasReduce = SetCurrentIndexStatsToImpl(index);
			return new IndexFailureInformation
			{
				Name = index,
				Attempts = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"]).Value,
				Errors = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"]).Value,
				Successes = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"]).Value,
				ReduceAttempts = hasReduce ? Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_attempts"]) : null,
				ReduceErrors = hasReduce ? Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_errors"]) : null,
				ReduceSuccesses = hasReduce ? Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_successes"]) : null
			};
		}

		public void UpdateLastIndexed(string index, Guid etag, DateTime timestamp)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
				throw new IndexDoesNotExistsException("There is no index named: " + index);

			using (var update = new Update(session, IndexesStats, JET_prep.Replace))
			{
				Api.SetColumn(session, IndexesStats,
							  tableColumnsCache.IndexesStatsColumns["last_indexed_etag"],
							  etag.TransformToValueForEsentSorting());
				Api.SetColumn(session, IndexesStats,
							  tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"],
							  timestamp.ToBinary());
				update.Save();
			}
		}

		public void UpdateIndexingStats(string index, IndexingWorkStats stats)
		{
			SetCurrentIndexStatsToImpl(index);
			using (var update = new Update(session, IndexesStats, JET_prep.Replace))
			{
				var oldAttempts = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"]) ?? 0;
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"],
					oldAttempts + stats.IndexingAttempts);

				var oldErrors = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"]) ?? 0;
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"],
					oldErrors + stats.IndexingErrors);

				var olsSuccesses = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"]) ?? 0;
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"],
					olsSuccesses + stats.IndexingSuccesses);

				update.Save();
			}
		}

		public void UpdateReduceStats(string index, IndexingWorkStats stats)
		{
			SetCurrentIndexStatsToImpl(index);
			using (var update = new Update(session, IndexesStatsReduce, JET_prep.Replace))
			{
				var oldAttempts = Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_attempts"]) ?? 0;
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_attempts"],
					oldAttempts + stats.ReduceAttempts);

				var oldErrors = Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_errors"]) ?? 0;
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_errors"],
					oldErrors + stats.ReduceErrors);

				var olsSuccesses = Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_successes"]) ?? 0;
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_successes"],
					olsSuccesses + stats.ReduceSuccesses);

				update.Save();
			}
		}

		public void TouchIndexEtag(string index)
		{
			Api.JetSetCurrentIndex(session, IndexesEtags, "by_key");
			Api.MakeKey(session, IndexesEtags, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesEtags, SeekGrbit.SeekEQ) == false)
				throw new IndexDoesNotExistsException("There is no reduce index named: " + index);

			Api.EscrowUpdate(session, IndexesEtags, tableColumnsCache.IndexesEtagsColumns["touches"], 1);
		}

		public void UpdateLastReduced(string index, Guid etag, DateTime timestamp)
		{
			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.MakeKey(session, IndexesStatsReduce, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ) == false)
				throw new IndexDoesNotExistsException("There is no reduce index named: " + index);

			using (var update = new Update(session, IndexesStatsReduce, JET_prep.Replace))
			{
				Api.SetColumn(session, IndexesStatsReduce,
							  tableColumnsCache.IndexesStatsReduceColumns["last_reduced_etag"],
							  etag.TransformToValueForEsentSorting());
				Api.SetColumn(session, IndexesStatsReduce,
							  tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"],
							  timestamp.ToBinary());
				update.Save();
			}
		}

		public void RemoveAllDocumentReferencesFrom(string key)
		{
			Api.JetSetCurrentIndex(session, IndexedDocumentsReferences, "by_key");
			Api.MakeKey(session, IndexedDocumentsReferences, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexedDocumentsReferences, SeekGrbit.SeekEQ) == false)
				return;
			Api.MakeKey(session, IndexedDocumentsReferences, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, IndexedDocumentsReferences,
			                     SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

			do
			{
				Api.JetDelete(session, IndexedDocumentsReferences);
			} while (Api.TryMoveNext(session, IndexedDocumentsReferences));
		}

		public void UpdateDocumentReferences(string view, string key, HashSet<string> references)
		{
			Api.JetSetCurrentIndex(session, IndexedDocumentsReferences, "by_view_and_key");
			Api.MakeKey(session, IndexedDocumentsReferences, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, IndexedDocumentsReferences, key, Encoding.Unicode, MakeKeyGrbit.None);
			if (Api.TrySeek(session, IndexedDocumentsReferences, SeekGrbit.SeekEQ))
			{
				Api.MakeKey(session, IndexedDocumentsReferences, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, IndexedDocumentsReferences, key, Encoding.Unicode, MakeKeyGrbit.None);
				Api.JetSetIndexRange(session, IndexedDocumentsReferences,
				                     SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
				do
				{
					var reference = Api.RetrieveColumnAsString(session, IndexedDocumentsReferences,
					                                           tableColumnsCache.IndexedDocumentsReferencesColumns["ref"],
					                                           Encoding.Unicode);

					if (references.Contains(reference))
					{
						references.Remove(reference);
						continue;
					}
					Api.JetDelete(session, IndexedDocumentsReferences);

				} while (Api.TryMoveNext(session, IndexedDocumentsReferences));
			}

			foreach (var reference in references)
			{
				using (var update = new Update(session, IndexedDocumentsReferences, JET_prep.Insert))
				{
					Api.SetColumn(session, IndexedDocumentsReferences, tableColumnsCache.IndexedDocumentsReferencesColumns["key"], key, Encoding.Unicode);
					Api.SetColumn(session, IndexedDocumentsReferences, tableColumnsCache.IndexedDocumentsReferencesColumns["view"], view, Encoding.Unicode);
					Api.SetColumn(session, IndexedDocumentsReferences, tableColumnsCache.IndexedDocumentsReferencesColumns["ref"], reference, Encoding.Unicode);
					update.Save();
				}
			}
		}

		public IEnumerable<string> GetDocumentsReferencing(string key)
		{
			return QueryReferences(key, "by_ref", "key");
		}

		public IEnumerable<string> GetDocumentsReferencesFrom(string key)
		{
			return QueryReferences(key, "by_key", "ref");
		}

		private IEnumerable<string> QueryReferences(string key, string index, string col)
		{
			Api.JetSetCurrentIndex(session, IndexedDocumentsReferences, index);
			Api.MakeKey(session, IndexedDocumentsReferences, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexedDocumentsReferences, SeekGrbit.SeekEQ) == false)
				return Enumerable.Empty<string>();
			Api.MakeKey(session, IndexedDocumentsReferences, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, IndexedDocumentsReferences,
			                     SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

			var results = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
			do
			{
				var item = Api.RetrieveColumnAsString(session, IndexedDocumentsReferences,
				                                        tableColumnsCache.IndexedDocumentsReferencesColumns[col], Encoding.Unicode);
				results.Add(item);
			} while (Api.TryMoveNext(session, IndexedDocumentsReferences));
			return results;
		}
	}
}
