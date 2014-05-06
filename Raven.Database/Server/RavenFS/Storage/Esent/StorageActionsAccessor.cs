//-----------------------------------------------------------------------
// <copyright file="StorageActionsAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Web;

using Microsoft.Isam.Esent.Interop;

using Raven.Abstractions.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage.Exceptions;
using Raven.Database.Server.RavenFS.Synchronization.Rdc;
using Raven.Database.Server.RavenFS.Util;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.RavenFS.Storage.Esent
{
    public class StorageActionsAccessor : IStorageActionsAccessor
    {
		private readonly JET_DBID database;
		private readonly Session session;
		private readonly TableColumnsCache tableColumnsCache;
		private Table config;
		private Table details;

		private Table files;
		private Table pages;
		private Table signatures;
		private Transaction transaction;
		private Table usage;

		public StorageActionsAccessor(TableColumnsCache tableColumnsCache, JET_INSTANCE instance, string databaseName)
		{
			this.tableColumnsCache = tableColumnsCache;
			try
			{
				session = new Session(instance);
				transaction = new Transaction(session);
				Api.JetOpenDatabase(session, databaseName, null, out database, OpenDatabaseGrbit.None);
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

		private Table Files
		{
			get { return files ?? (files = new Table(session, database, "files", OpenTableGrbit.None)); }
		}

		private Table Signatures
		{
			get { return signatures ?? (signatures = new Table(session, database, "signatures", OpenTableGrbit.None)); }
		}

		private Table Config
		{
			get { return config ?? (config = new Table(session, database, "config", OpenTableGrbit.None)); }
		}

		private Table Usage
		{
			get { return usage ?? (usage = new Table(session, database, "usage", OpenTableGrbit.None)); }
		}

		private Table Pages
		{
			get { return pages ?? (pages = new Table(session, database, "pages", OpenTableGrbit.None)); }
		}

		private Table Details
		{
			get { return details ?? (details = new Table(session, database, "details", OpenTableGrbit.None)); }
		}


		[DebuggerHidden]
		[DebuggerNonUserCode]
		public void Dispose()
		{
			if (signatures != null)
				signatures.Dispose();
			if (config != null)
				config.Dispose();
			if (details != null)
				details.Dispose();
			if (pages != null)
				pages.Dispose();
			if (usage != null)
				usage.Dispose();
			if (files != null)
				files.Dispose();
			if (Equals(database, JET_DBID.Nil) == false)
				Api.JetCloseDatabase(session, database, CloseDatabaseGrbit.None);
			if (transaction != null)
				transaction.Dispose();
			if (session != null)
				session.Dispose();
		}

		[DebuggerHidden]
		[DebuggerNonUserCode]
		public void Commit()
		{
			transaction.Commit(CommitTransactionGrbit.None);
		}

		public void PulseTransaction()
		{
			transaction.Commit(CommitTransactionGrbit.LazyFlush);
			transaction.Dispose();
			transaction = new Transaction(session);
		}

		public int InsertPage(byte[] buffer, int size)
		{
			var key = new HashKey(buffer, size);

			Api.JetSetCurrentIndex(session, Pages, "by_keys");

			Api.MakeKey(session, Pages, key.Weak, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Pages, key.Strong, MakeKeyGrbit.None);

			if (Api.TrySeek(session, Pages, SeekGrbit.SeekEQ))
			{
				Api.EscrowUpdate(session, Pages, tableColumnsCache.PagesColumns["usage_count"], 1);
				return Api.RetrieveColumnAsInt32(session, Pages, tableColumnsCache.PagesColumns["id"]).Value;
			}

			var bookMarkBuffer = new byte[SystemParameters.BookmarkMost];
			var actualSize = 0;
			using (var update = new Update(session, Pages, JET_prep.Insert))
			{
				Api.SetColumn(session, Pages, tableColumnsCache.PagesColumns["page_strong_hash"], key.Strong);
				Api.SetColumn(session, Pages, tableColumnsCache.PagesColumns["page_weak_hash"], key.Weak);
				Api.JetSetColumn(session, Pages, tableColumnsCache.PagesColumns["data"], buffer, size, SetColumnGrbit.None, null);

				try
				{
					update.Save(bookMarkBuffer, bookMarkBuffer.Length, out actualSize);
				}
				catch (EsentKeyDuplicateException)
				{
					// it means that page is being inserted by another thread
					throw new ConcurrencyException("The same file page is being created");
				}
			}

			Api.JetGotoBookmark(session, Pages, bookMarkBuffer, actualSize);

			return Api.RetrieveColumnAsInt32(session, Pages, tableColumnsCache.PagesColumns["id"]).Value;
		}

        public void PutFile(string filename, long? totalSize, RavenJObject metadata, bool tombstone = false)
        {
            using (var update = new Update(session, Files, JET_prep.Insert))
            {
                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["name"], filename, Encoding.Unicode);
                if (totalSize != null)
                    Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["total_size"], BitConverter.GetBytes(totalSize.Value));

                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"], BitConverter.GetBytes(0));

                if (!metadata.ContainsKey("ETag"))
                    throw new InvalidOperationException(string.Format("Metadata of file {0} does not contain 'ETag' key", filename));

                var innerEsentMetadata = new RavenJObject(metadata);
                var etag = innerEsentMetadata.Value<Guid>("ETag");
                innerEsentMetadata.Remove("ETag");

                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["etag"], etag.TransformToValueForEsentSorting());
                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["metadata"], ToQueryString(innerEsentMetadata), Encoding.Unicode);

                update.Save();
            }

            if (!tombstone)
            {
                if (Api.TryMoveFirst(session, Details) == false)
                    throw new InvalidOperationException("Could not find system metadata row");

                Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["file_count"], 1);
            }
        }

        private static string ToQueryString(RavenJObject metadata)
        {
            var serializer = JsonExtensions.CreateDefaultJsonSerializer();
            var sb = new StringBuilder();
            serializer.Serialize(new JsonTextWriter(new StringWriter(sb)), metadata);

            return sb.ToString();
        }

		public void AssociatePage(string filename, int pageId, int pagePositionInFile, int pageSize)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
				throw new FileNotFoundException("Could not find file: " + filename);

			using (var update = new Update(session, Files, JET_prep.Replace))
			{
				var totalSize = GetTotalSize();
				var uploadedSize =
					BitConverter.ToInt64(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0);

				if (totalSize != null && totalSize >= 0 && uploadedSize + pageSize > totalSize)
					throw new InvalidDataException("Try to upload more data than the file was allocated for (" + totalSize +
					                               ") and new size would be: " + (uploadedSize + pageSize));

				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"],
				              BitConverter.GetBytes(uploadedSize + pageSize));

				// using chunked encoding, we don't know what the size is
				// we use negative values here for keeping track of the unknown size
				if (totalSize == null || totalSize < 0)
				{
					var actualSize = totalSize ?? 0;
					Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["total_size"],
					              BitConverter.GetBytes(actualSize - pageSize));
				}

				update.Save();
			}

			using (var update = new Update(session, Usage, JET_prep.Insert))
			{
				Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["name"], filename, Encoding.Unicode);
				Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["file_pos"], pagePositionInFile);
				Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["page_id"], pageId);
				Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["page_size"], pageSize);

				update.Save();
			}
		}

		private long? GetTotalSize()
		{
			var totalSize = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["total_size"]);
			if (totalSize == null)
				return null;

			return BitConverter.ToInt64(totalSize, 0);
		}

		public int ReadPage(int pageId, byte[] buffer)
		{
			Api.JetSetCurrentIndex(session, Pages, "by_id");
			Api.MakeKey(session, Pages, pageId, MakeKeyGrbit.NewKey);

			if (Api.TrySeek(session, Pages, SeekGrbit.SeekEQ) == false)
				return -1;

			int size;
			Api.JetRetrieveColumn(session, Pages, tableColumnsCache.PagesColumns["data"], buffer, buffer.Length, out size,
			                      RetrieveColumnGrbit.None, null);
			return size;
		}

		public FileHeader ReadFile(string filename)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
				return null;

			return new FileHeader
				       {
					       Name = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"], Encoding.Unicode),
					       TotalSize = GetTotalSize(),
					       UploadedSize = BitConverter.ToInt64(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0),
					       Metadata = RetrieveMetadata()
				       };
		}

		public FileAndPages GetFile(string filename, int start, int pagesToLoad)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
				throw new FileNotFoundException("Could not find file: " + filename);

			var fileInformation = new FileAndPages
				                      {
					                      TotalSize = GetTotalSize(),
					                      UploadedSize = BitConverter.ToInt64(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0),
					                      Metadata = RetrieveMetadata(),
					                      Name = filename,
					                      Start = start
				                      };

			if (pagesToLoad > 0)
			{
				Api.JetSetCurrentIndex(session, Usage, "by_name_and_pos");
				Api.MakeKey(session, Usage, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, Usage, start, MakeKeyGrbit.None);
				if (Api.TrySeek(session, Usage, SeekGrbit.SeekGE))
				{
					Api.MakeKey(session, Usage, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
					Api.JetSetIndexRange(session, Usage, SetIndexRangeGrbit.RangeInclusive);

					do
					{
						var name = Api.RetrieveColumnAsString(session, Usage, tableColumnsCache.UsageColumns["name"]);
						if (name != filename)
							continue;

						fileInformation.Pages.Add(new PageInformation
							                          {
								                          Size = Api.RetrieveColumnAsInt32(session, Usage, tableColumnsCache.UsageColumns["page_size"]).Value,
								                          Id = Api.RetrieveColumnAsInt32(session, Usage, tableColumnsCache.UsageColumns["page_id"]).Value
							                          });
					} while (Api.TryMoveNext(session, Usage) && fileInformation.Pages.Count < pagesToLoad);
				}
			}

			return fileInformation;
		}

		public IEnumerable<FileHeader> ReadFiles(int start, int size)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			if (Api.TryMoveFirst(session, Files) == false)
				yield break;

			try
			{
				Api.JetMove(session, Files, start, MoveGrbit.None);
			}
			catch (EsentErrorException e)
			{
				if (e.Error == JET_err.NoCurrentRecord)
					yield break;
				throw;
			}

			var index = 0;

			do
			{
				yield return new FileHeader
					             {
						             Name = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"], Encoding.Unicode),
						             TotalSize = GetTotalSize(),
						             UploadedSize = BitConverter.ToInt64(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0),
						             Metadata = RetrieveMetadata()
					             };
			} while (++index < size && Api.TryMoveNext(session, Files));
		}

        private RavenJObject RetrieveMetadata()
        {
            var metadataAsString = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["metadata"], Encoding.Unicode);

            var metadata = RavenJObject.Parse(metadataAsString);
            metadata["ETag"] = new RavenJValue(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]).TransfromToGuidWithProperSorting());
            
            return metadata;
        }

		public IEnumerable<FileHeader> GetFilesAfter(Guid etag, int take)
		{
			Api.JetSetCurrentIndex(session, Files, "by_etag");
			Api.MakeKey(session, Files, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekGT) == false)
				return Enumerable.Empty<FileHeader>();

			var result = new List<FileHeader>();
			var index = 0;

			do
			{
				result.Add(new FileHeader
					           {
						           Name = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"], Encoding.Unicode),
						           TotalSize = GetTotalSize(),
						           UploadedSize = BitConverter.ToInt64( Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0),
						           Metadata = RetrieveMetadata()
					           });
			} while (++index < take && Api.TryMoveNext(session, Files));

			return result;
		}

		public void Delete(string filename)
		{
			Api.JetSetCurrentIndex(session, Usage, "by_name_and_pos");
			Api.MakeKey(session, Usage, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Usage, SeekGrbit.SeekGE))
			{
				Api.JetSetCurrentIndex(session, Pages, "by_id");

				var count = 0;
				do
				{
					var rowName = Api.RetrieveColumnAsString(session, Usage, tableColumnsCache.UsageColumns["name"]);
					if (rowName != filename)
						break;

					var pageId = Api.RetrieveColumnAsInt32(session, Usage, tableColumnsCache.UsageColumns["page_id"]).Value;

					Api.MakeKey(session, Pages, pageId, MakeKeyGrbit.NewKey);

					if (Api.TrySeek(session, Pages, SeekGrbit.SeekEQ))
					{
						var escrowUpdate = Api.EscrowUpdate(session, Pages, tableColumnsCache.PagesColumns["usage_count"], -1);
						if (escrowUpdate <= 1)
						{
							Api.JetDelete(session, Pages);
						}
					}

					Api.JetDelete(session, Usage);

					if (count++ > 1000)
					{
						PulseTransaction();
						count = 0;
					}
				} while (Api.TryMoveNext(session, Usage));
			}

			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
				return;

			Api.JetDelete(session, Files);
		}

        public void UpdateFileMetadata(string filename, RavenJObject metadata)
        {
            Api.JetSetCurrentIndex(session, Files, "by_name");
            Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
                throw new FileNotFoundException(filename);

            using (var update = new Update(session, Files, JET_prep.Replace))
            {
                if (!metadata.ContainsKey("ETag"))
                {
                    throw new InvalidOperationException("Metadata of file {0} does not contain 'ETag' key " + filename);
                }

                var innerEsentMetadata = new RavenJObject(metadata);
                var etag = innerEsentMetadata.Value<Guid>("ETag");
                innerEsentMetadata.Remove("ETag");

                var existingMetadata = RetrieveMetadata();

                if (existingMetadata.ContainsKey("Content-MD5"))
                {
                    innerEsentMetadata["Content-MD5"] = existingMetadata["Content-MD5"];
                }

                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["etag"], etag.TransformToValueForEsentSorting());
                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["metadata"], ToQueryString(innerEsentMetadata), Encoding.Unicode);

                update.Save();
            }
        }


		public void CompleteFileUpload(string filename)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
				throw new FileNotFoundException("Could not find file: " + filename);

			using (var update = new Update(session, Files, JET_prep.Replace))
			{
				var totalSize = GetTotalSize() ?? 0;
                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["total_size"],
				              BitConverter.GetBytes(Math.Abs(totalSize)));

				update.Save();
			}
		}

		public int GetFileCount()
		{
			if (Api.TryMoveFirst(session, Details) == false)
				throw new InvalidOperationException("Could not find system metadata row");

			return Api.RetrieveColumnAsInt32(session, Details, tableColumnsCache.DetailsColumns["file_count"]).Value;
		}

		public void DecrementFileCount(string nameOfFileThatShouldNotBeCounted)
		{
			if (Api.TryMoveFirst(session, Details) == false)
				throw new InvalidOperationException("Could not find system metadata row");

			Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["file_count"], -1);
		}

		public void RenameFile(string filename, string rename, bool commitPeriodically = false)
		{
		    try
		    {
                Api.JetSetCurrentIndex(session, Usage, "by_name_and_pos");
                Api.MakeKey(session, Usage, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
                if (Api.TrySeek(session, Usage, SeekGrbit.SeekGE))
                {
                    var count = 0;
                    do
                    {
                        var name = Api.RetrieveColumnAsString(session, Usage, tableColumnsCache.UsageColumns["name"]);
                        if (name != filename)
                            break;

                        using (var update = new Update(session, Usage, JET_prep.Replace))
                        {
                            Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["name"], rename, Encoding.Unicode);

                            update.Save();
                        }

                        if (commitPeriodically && count++ > 1000)
                        {
                            PulseTransaction();
                            count = 0;
                        }
                    } while (Api.TryMoveNext(session, Usage));
                }

                Api.JetSetCurrentIndex(session, Files, "by_name");
                Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
                if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
                    throw new FileNotFoundException("Could not find file: " + filename);

                using (var update = new Update(session, Files, JET_prep.Replace))
                {
                    Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["name"], rename, Encoding.Unicode);

                    update.Save();
                }
		    }
		    catch (Exception e)
		    {
		        if (e is EsentKeyDuplicateException)
                    throw new FileExistsException(string.Format("Cannot rename '{0}' to '{1}'. Rename '{1}' exists.", filename, rename), e);

		        throw;
		    }
		}

        public RavenJObject GetConfig(string name)
		{
			Api.JetSetCurrentIndex(session, Config, "by_name");
			Api.MakeKey(session, Config, name, Encoding.Unicode, MakeKeyGrbit.NewKey);			
            if (Api.TrySeek(session, Config, SeekGrbit.SeekEQ) == false)
				throw new FileNotFoundException("Could not find config: " + name);
			
            var metadata = Api.RetrieveColumnAsString(session, Config, tableColumnsCache.ConfigColumns["metadata"], Encoding.Unicode);
            return RavenJObject.Parse(metadata);
		}

        public void SetConfig(string name, RavenJObject metadata)
		{
            var builder = new StringBuilder();
            using (var writer = new JsonTextWriter(new StringWriter(builder)))
                metadata.WriteTo(writer);

            string metadataString = builder.ToString();            
            
            Api.JetSetCurrentIndex(session, Config, "by_name");
			Api.MakeKey(session, Config, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var prep = Api.TrySeek(session, Config, SeekGrbit.SeekEQ) ? JET_prep.Replace : JET_prep.Insert;

			using (var update = new Update(session, Config, prep))            
			{
				Api.SetColumn(session, Config, tableColumnsCache.ConfigColumns["name"], name, Encoding.Unicode);
                Api.SetColumn(session, Config, tableColumnsCache.ConfigColumns["metadata"], metadataString, Encoding.Unicode);

				update.Save();
			}
		}

		public void DeleteConfig(string name)
		{
			Api.JetSetCurrentIndex(session, Config, "by_name");
			Api.MakeKey(session, Config, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Config, SeekGrbit.SeekEQ) == false)
				return;

			Api.JetDelete(session, Config);
		}

		public IEnumerable<SignatureLevels> GetSignatures(string name)
		{
			Api.JetSetCurrentIndex(session, Signatures, "by_name");
			Api.MakeKey(session, Signatures, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Signatures, SeekGrbit.SeekEQ) == false)
				yield break;

			Api.MakeKey(session, Signatures, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.TrySetIndexRange(session, Signatures, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

			do
			{
				yield return new SignatureLevels
					             {
						             Id = Api.RetrieveColumnAsInt32(session, Signatures, tableColumnsCache.SignaturesColumns["id"]).Value,
						             Level =
							             Api.RetrieveColumnAsInt32(session, Signatures, tableColumnsCache.SignaturesColumns["level"]).Value,
						             CreatedAt =
							             Api.RetrieveColumnAsDateTime(session, Signatures, tableColumnsCache.SignaturesColumns["created_at"])
							                .Value
					             };
			} while (Api.TryMoveNext(session, Signatures));
		}

		public void ClearSignatures(string name)
		{
			Api.JetSetCurrentIndex(session, Signatures, "by_name");
			Api.MakeKey(session, Signatures, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Signatures, SeekGrbit.SeekEQ) == false)
				return;

			Api.MakeKey(session, Signatures, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.TrySetIndexRange(session, Signatures, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

			do
			{
				Api.JetDelete(session, Signatures);
			} while (Api.TryMoveNext(session, Signatures));
		}


		public long GetSignatureSize(int id, int level)
		{
			Api.JetSetCurrentIndex(session, Signatures, "by_id");
			Api.MakeKey(session, Signatures, id, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Signatures, level, MakeKeyGrbit.None);
			if (Api.TrySeek(session, Signatures, SeekGrbit.SeekEQ) == false)
				throw new InvalidOperationException("Could not find signature with id " + id + " and level " + level);

			return Api.RetrieveColumnSize(session, Signatures, tableColumnsCache.SignaturesColumns["data"]) ?? 0;
		}

		public void GetSignatureStream(int id, int level, Action<Stream> action)
		{
			Api.JetSetCurrentIndex(session, Signatures, "by_id");
			Api.MakeKey(session, Signatures, id, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Signatures, level, MakeKeyGrbit.None);
			if (Api.TrySeek(session, Signatures, SeekGrbit.SeekEQ) == false)
				throw new InvalidOperationException("Could not find signature with id " + id + " and level " + level);


			using (var stream = new ColumnStream(session, Signatures, tableColumnsCache.SignaturesColumns["data"]))
			using (var buffer = new BufferedStream(stream))
			{
				action(buffer);
				buffer.Flush();
				stream.Flush();
			}
		}

		public void AddSignature(string name, int level, Action<Stream> action)
		{
			using (var update = new Update(session, Signatures, JET_prep.Insert))
			{
				Api.SetColumn(session, Signatures, tableColumnsCache.SignaturesColumns["name"], name, Encoding.Unicode);
				Api.SetColumn(session, Signatures, tableColumnsCache.SignaturesColumns["level"], level);
				Api.SetColumn(session, Signatures, tableColumnsCache.SignaturesColumns["created_at"], DateTime.UtcNow);

				using (var stream = new ColumnStream(session, Signatures, tableColumnsCache.SignaturesColumns["data"]))
				using (var buffer = new BufferedStream(stream))
				{
					action(buffer);
					buffer.Flush();
					stream.Flush();
				}

				update.Save();
			}
		}

		public IEnumerable<string> GetConfigNames(int start, int pageSize)
		{
			Api.JetSetCurrentIndex(session, Config, "by_name");
			Api.MoveBeforeFirst(session, Config);
			for (var i = 0; i < start; i++)
			{
				if (Api.TryMoveNext(session, Config) == false)
					yield break;
			}

			int count = 0;
			while (Api.TryMoveNext(session, Config) && count < pageSize)
			{
				yield return Api.RetrieveColumnAsString(session, Config, tableColumnsCache.ConfigColumns["name"]);
			    count++;
			}
		}

		public bool ConfigExists(string name)
		{
			Api.JetSetCurrentIndex(session, Config, "by_name");
			Api.MakeKey(session, Config, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			return Api.TrySeek(session, Config, SeekGrbit.SeekEQ);
		}

        public IList<RavenJObject> GetConfigsStartWithPrefix(string prefix, int start, int take)
		{
            var configs = new List<RavenJObject>();

			Api.JetSetCurrentIndex(session, Config, "by_name");

			Api.MakeKey(session, Config, prefix, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Config, SeekGrbit.SeekGE) == false)
			{
				return configs;
			}

			Api.MakeKey(session, Config, prefix, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);
			try
			{
				Api.JetMove(session, Config, start, MoveGrbit.MoveKeyNE);
			}
			catch (EsentNoCurrentRecordException)
			{
				return configs;
			}

			if (Api.TrySetIndexRange(session, Config, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit))
			{
				do
				{
					var metadata = Api.RetrieveColumnAsString(session, Config, tableColumnsCache.ConfigColumns["metadata"], Encoding.Unicode);
                    configs.Add(RavenJObject.Parse(metadata));
				} 
                while (Api.TryMoveNext(session, Config) && configs.Count < take);
			}

			return configs;
		}

		public IList<string> GetConfigNamesStartingWithPrefix(string prefix, int start, int take, out int total)
		{
			var configs = new List<string>();

			Api.JetSetCurrentIndex(session, Config, "by_name");

			if (!string.IsNullOrEmpty(prefix))
			{
				Api.MakeKey(session, Config, prefix, Encoding.Unicode, MakeKeyGrbit.NewKey);
				if (Api.TrySeek(session, Config, SeekGrbit.SeekGE) == false)
				{
					total = 0;
					return configs;
				}

				Api.MakeKey(session, Config, prefix, Encoding.Unicode,
				            MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);
				try
				{
					Api.JetMove(session, Config, 0, MoveGrbit.MoveKeyNE);
				}
				catch (EsentNoCurrentRecordException)
				{
					total = 0;
					return configs;
				}

				if (!Api.TrySetIndexRange(session, Config,
				                          SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit))
				{
					total = 0;
					return configs;
				}
			}
			else
			{
				if (!Api.TryMoveFirst(session, Config))
				{
					total = 0;
					return configs;
				}
			}

			var skippedCount = 0;

			for (int i = 0; i < start; i++)
			{
				if (Api.TryMoveNext(session, Config) == false)
				{
					total = skippedCount;
					return configs;
				}
				skippedCount++;
			}

			var hasNextRecord = false;
			do
			{
				var configName = Api.RetrieveColumnAsString(session, Config, tableColumnsCache.ConfigColumns["name"]);
				configs.Add(configName);
				hasNextRecord = Api.TryMoveNext(session, Config);
			} while (hasNextRecord && configs.Count < take);

			var extraRecords = 0;
			if (hasNextRecord)
			{
				Api.JetIndexRecordCount(session, Config, out extraRecords, 0);
			}
			total = skippedCount + configs.Count + extraRecords;

			return configs;
		}
	}
}