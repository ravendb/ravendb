// -----------------------------------------------------------------------
//  <copyright file="SchemaCreator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.FileSystem.Storage.Voron.Impl;
using Raven.Database.Util;

using Voron;
using Voron.Impl;

namespace Raven.Database.FileSystem.Storage.Voron.Schema
{
	internal class SchemaCreator
	{
		private readonly TableStorage storage;

		private readonly Action<string> output;

		private readonly ILog log;

		public const string SchemaVersion = "1.1";

		[ImportMany]
		public OrderedPartCollection<ISchemaUpdate> Updaters { get; set; }

		private static readonly object UpdateLocker = new object();

		public SchemaCreator(InMemoryRavenConfiguration configuration, TableStorage storage, Action<string> output, ILog log)
		{
			this.storage = storage;
			this.output = output;
			this.log = log;

			configuration.Container.SatisfyImportsOnce(this);
		}

		//create all relevant storage trees in one place
		public void CreateSchema()
		{
			using (var tx = storage.Environment.NewTransaction(TransactionFlags.ReadWrite))
			{
				CreateFilesSchema(tx, storage);
				CreatSignaturesSchema(tx, storage);
				CreateConfigSchema(tx, storage);
				CreateUsageSchema(tx, storage);
				CreatePagesSchema(tx, storage);
				CreateDetailsSchema(tx, storage);

				tx.Commit();
			}
		}

		public void SetupDatabaseIdAndSchemaVersion()
		{
			using (var snapshot = storage.CreateSnapshot())
			{
				Guid id;
				string schemaVersion;

                Slice idKey = new Slice ("id");
                Slice schemaVersionKey = new Slice("schema_version");

                var read = storage.Details.Read(snapshot, idKey, null);
				if (read == null) // new db
				{
					id = Guid.NewGuid();
					schemaVersion = SchemaVersion;
					using (var writeIdBatch = new WriteBatch())
					{
                        storage.Details.Add(writeIdBatch, idKey, id.ToByteArray());
                        storage.Details.Add(writeIdBatch, schemaVersionKey, schemaVersion);
						storage.Write(writeIdBatch);
					}
				}
				else
				{
                    if (read.Reader.Length != 16) //precaution - might prevent NRE in edge cases
                        throw new InvalidDataException("Failed to initialize Voron transactional storage. Possible data corruption. (no db id)");

                    using (var stream = read.Reader.AsStream())
                    using (var reader = new BinaryReader(stream))
                    {
                        id = new Guid(reader.ReadBytes((int)stream.Length));
                    }

                    var schemaRead = storage.Details.Read(snapshot, schemaVersionKey, null);
                    if (schemaRead == null)
                        throw new InvalidDataException("Failed to initialize Voron transactional storage. Possible data corruption. (no schema version)");

					schemaVersion = schemaRead.Reader.ToStringValue();
				}

				storage.SetDatabaseIdAndSchemaVersion(id, schemaVersion);
			}
		}

		public void UpdateSchemaIfNecessary()
		{
			if (storage.SchemaVersion == SchemaVersion)
				return;

			using (var ticker = new OutputTicker(TimeSpan.FromSeconds(3), () =>
			{
				log.Info(".");
				Console.Write(".");
			}, null, () =>
			{
				log.Info("OK");
				Console.Write("OK");
				Console.WriteLine();
			}))
			{
				bool lockTaken = false;
				try
				{
					Monitor.TryEnter(UpdateLocker, TimeSpan.FromSeconds(15), ref lockTaken);
					if (lockTaken == false)
						throw new TimeoutException("Could not take upgrade lock after 15 seconds, probably another database is upgrading itself and we can't interupt it midway. Please try again later");

					do
					{
						var updater = Updaters.FirstOrDefault(update => update.Value.FromSchemaVersion == storage.SchemaVersion);
						if (updater == null)
							throw new InvalidOperationException(
								string.Format(
									"The version on disk ({0}) is different that the version supported by this library: {1}{2}You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.",
									storage.SchemaVersion, SchemaVersion, Environment.NewLine));

						log.Info("Updating schema from version {0}: ", storage.SchemaVersion);
						Console.WriteLine("Updating schema from version {0}: ", storage.SchemaVersion);

						ticker.Start();

						updater.Value.Update(storage, output);
						updater.Value.UpdateSchemaVersion(storage, output);

						ticker.Stop();

					} while (storage.SchemaVersion != SchemaVersion);
				}
				finally
				{
					if (lockTaken)
						Monitor.Exit(UpdateLocker);
				}
			}
		}

		private static void CreatePagesSchema(Transaction tx, TableStorage storage)
		{
			storage.Environment.CreateTree(tx, Tables.Pages.TableName);
			storage.Environment.CreateTree(tx, storage.Pages.GetIndexKey(Tables.Pages.Indices.Data));
			storage.Environment.CreateTree(tx, storage.Pages.GetIndexKey(Tables.Pages.Indices.ByKey));
		}

		private static void CreateUsageSchema(Transaction tx, TableStorage storage)
		{
			storage.Environment.CreateTree(tx, Tables.Usage.TableName);
			storage.Environment.CreateTree(tx, storage.Usage.GetIndexKey(Tables.Usage.Indices.ByFileName));
			storage.Environment.CreateTree(tx, storage.Usage.GetIndexKey(Tables.Usage.Indices.ByFileNameAndPosition));
		}

		private static void CreateConfigSchema(Transaction tx, TableStorage storage)
		{
			storage.Environment.CreateTree(tx, Tables.Config.TableName);
		}

		private static void CreatSignaturesSchema(Transaction tx, TableStorage storage)
		{
			storage.Environment.CreateTree(tx, Tables.Signatures.TableName);
			storage.Environment.CreateTree(tx, storage.Signatures.GetIndexKey(Tables.Signatures.Indices.Data));
			storage.Environment.CreateTree(tx, storage.Signatures.GetIndexKey(Tables.Signatures.Indices.ByName));
		}

		private static void CreateFilesSchema(Transaction tx, TableStorage storage)
		{
			storage.Environment.CreateTree(tx, Tables.Files.TableName);
			storage.Environment.CreateTree(tx, storage.Files.GetIndexKey(Tables.Files.Indices.Count));
			storage.Environment.CreateTree(tx, storage.Files.GetIndexKey(Tables.Files.Indices.ByEtag));
		}

		private static void CreateDetailsSchema(Transaction tx, TableStorage storage)
		{
			storage.Environment.CreateTree(tx, Tables.Details.TableName);
		}
	}
}