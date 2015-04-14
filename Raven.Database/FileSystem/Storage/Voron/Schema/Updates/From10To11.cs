// -----------------------------------------------------------------------
//  <copyright file="From10To11.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Database.FileSystem.Storage.Voron.Impl;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;
using Voron;

namespace Raven.Database.FileSystem.Storage.Voron.Schema.Updates
{
	internal class From10To11 : SchemaUpdateBase
	{
		public override string FromSchemaVersion
		{
			get { return "1.0"; }
		}
		public override string ToSchemaVersion
		{
			get { return "1.1"; }
		}

		public override void Update(TableStorage tableStorage, Action<string> output)
		{
			Migrate(tableStorage.Environment, Tables.Files.TableName, output, (key, value) =>
			{
				var fileEtag = Guid.Parse(value["etag"].ToString());

				value["etag"] = fileEtag.ToByteArray();
			});

			Migrate(tableStorage.Environment, Tables.Config.TableName, output, (key, value) =>
			{
				var actualKey = key.ToString();

				if (actualKey.StartsWith(RavenFileNameHelper.SyncNamePrefix, StringComparison.InvariantCultureIgnoreCase))
				{
					string currentEtag = ((RavenJObject) value["metadata"])["FileETag"].ToString();

					((RavenJObject) value["metadata"])["FileETag"] = Etag.Parse(Guid.Parse(currentEtag).ToByteArray()).ToString();
				}
				else if (actualKey.StartsWith(RavenFileNameHelper.SyncResultNamePrefix, StringComparison.InvariantCultureIgnoreCase))
				{
					string currentEtag = ((RavenJObject) value["metadata"])["FileETag"].ToString();

					((RavenJObject) value["metadata"])["FileETag"] = Etag.Parse(Guid.Parse(currentEtag).ToByteArray()).ToString();
				}
				else if (actualKey.StartsWith(SynchronizationConstants.RavenSynchronizationSourcesBasePath, StringComparison.InvariantCultureIgnoreCase))
				{
					string currentEtag = ((RavenJObject) value["metadata"])["LastSourceFileEtag"].ToString();

					((RavenJObject) value["metadata"])["LastSourceFileEtag"] = Etag.Parse(Guid.Parse(currentEtag).ToByteArray()).ToString();
				}
			});

			UpdateSchemaVersion(tableStorage, output);
		}


		private static void Migrate(StorageEnvironment env, string tableName, Action<string> output, Action<Slice, RavenJObject> modifyRecord)
		{
			long entriesCount;

			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				entriesCount = tx.ReadTree(tableName).State.EntriesCount;
			}

			if (entriesCount == 0)
			{
				output(string.Format("No records to migrate in '{0}' table.", tableName));
				return;
			}

			output(string.Format("Starting to migrate '{0}' table to. Records to process: {1}", tableName, entriesCount));


			using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				env.DeleteTree(txw, "Temp_" + tableName);

				txw.Commit();
			}

			using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				env.CreateTree(txw, "Temp_" + tableName);

				txw.Commit();
			}

			var migrated = 0L;
			var keyToSeek = Slice.BeforeAllKeys;

			do
			{
				using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var destTree = txw.ReadTree("Temp_" + tableName);
					var srcTree = txw.ReadTree(tableName);

					var iterator = srcTree.Iterate();

					if (iterator.Seek(keyToSeek) == false)
						break;

					var itemsInBatch = 0;

					do
					{
						keyToSeek = iterator.CurrentKey;

						if (itemsInBatch != 0 && itemsInBatch % 100 == 0)
							break;

						using (var stream = iterator.CreateReaderForCurrent().AsStream())
						{
							var value = stream.ToJObject();

							modifyRecord(iterator.CurrentKey, value);

							using (var streamValue = new MemoryStream())
							{
								value.WriteTo(streamValue);
								streamValue.Position = 0;

								destTree.Add(iterator.CurrentKey, streamValue);
							}

							migrated++;
							itemsInBatch++;
						}
					} while (iterator.MoveNext());

					txw.Commit();

					output(string.Format("{0} of {1} entries processed.", migrated, entriesCount));
				}
			} while (migrated < entriesCount);

			using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				env.DeleteTree(txw, tableName);
				env.RenameTree(txw, "Temp_" + tableName, tableName);

				txw.Commit();
			}
		}
	}
}