// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3759.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Impl.Files;
using Raven.Smuggler.Database.Impl.Remote;
using Raven.Smuggler.Database.Impl.Streams;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3759 : RavenTest
	{
		[Fact]
		public async Task NorthwindStreamReadBasicTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				using (var input = new MemoryStream())
				{
					var oldSmuggler = new SmugglerDatabaseApi();
					await oldSmuggler
						.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
						{
							From = new RavenConnectionStringOptions
							{
								DefaultDatabase = store.DefaultDatabase,
								Url = store.Url
							},
							ToStream = input
						});

					var destination = new DatabaseSmugglerCountingDestination();
					var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerStreamSource(input, CancellationToken.None), destination);
					await smuggler.ExecuteAsync();

					Assert.Equal(1059, destination.WroteDocuments);
					Assert.Equal(0, destination.WroteDocumentDeletions);
					Assert.Equal(1, destination.WroteIdentities);
					Assert.Equal(4, destination.WroteIndexes);
					Assert.Equal(1, destination.WroteTransformers);
				}
			}
		}

		[Fact]
		public async Task NorthwindFileReadBasicTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				var input = Path.Combine(NewDataPath(forceCreateDir: true), "backup.ravendump");

				var oldSmuggler = new SmugglerDatabaseApi();
				await oldSmuggler
					.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
					{
						From = new RavenConnectionStringOptions
						{
							DefaultDatabase = store.DefaultDatabase,
							Url = store.Url
						},
						ToFile = input
					});

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(input, CancellationToken.None), destination);
				await smuggler.ExecuteAsync();

				Assert.Equal(1059, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(1, destination.WroteIdentities);
				Assert.Equal(4, destination.WroteIndexes);
				Assert.Equal(1, destination.WroteTransformers);
			}
		}

		[Fact]
		public async Task NorthwindRemoteReadBasicTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteSource(store, CancellationToken.None), destination);
				await smuggler.ExecuteAsync();

				Assert.Equal(1059, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(1, destination.WroteIdentities);
				Assert.Equal(4, destination.WroteIndexes);
				Assert.Equal(1, destination.WroteTransformers);
			}
		}

		[Fact]
		public async Task NorthwindFileReadIncrementalTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				var input = NewDataPath(forceCreateDir: true);

				var oldSmuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions { Incremental = true });
				await oldSmuggler
					.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
					{
						From = new RavenConnectionStringOptions
						{
							DefaultDatabase = store.DefaultDatabase,
							Url = store.Url
						},
						ToFile = input
					});

				using (var session = store.OpenSession())
				{
					session.Store(new Person { Name = "John Doe" });
					session.SaveChanges();
				}

				await oldSmuggler
					.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
					{
						From = new RavenConnectionStringOptions
						{
							DefaultDatabase = store.DefaultDatabase,
							Url = store.Url
						},
						ToFile = input
					});

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(input, CancellationToken.None), destination);
				await smuggler.ExecuteAsync();

				Assert.Equal(1061, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(2, destination.WroteIdentities);
				Assert.Equal(4, destination.WroteIndexes);
				Assert.Equal(1, destination.WroteTransformers);
			}
		}

		private class DatabaseSmugglerCountingDestination : IDatabaseSmugglerDestination
		{
			private readonly DatabaseSmugglerCountingIndexActions _indexActions;

			private readonly DatabaseSmugglerCountingDocumentActions _documentActions;

			private readonly DatabaseSmugglerCountingTransformerActions _transformerActions;

			private readonly DatabaseSmugglerCountingDocumentDeletionActions _documentDeletionActions;

			private readonly DatabaseSmugglerCountingIdentityActions _identityActions;

			public DatabaseSmugglerCountingDestination()
			{
				_indexActions = new DatabaseSmugglerCountingIndexActions();
				_documentActions = new DatabaseSmugglerCountingDocumentActions();
				_transformerActions = new DatabaseSmugglerCountingTransformerActions();
				_documentDeletionActions = new DatabaseSmugglerCountingDocumentDeletionActions();
				_identityActions = new DatabaseSmugglerCountingIdentityActions();
			}

			public int WroteIndexes => _indexActions.Count;

			public int WroteDocuments => _documentActions.Count;

			public int WroteTransformers => _transformerActions.Count;

			public int WroteDocumentDeletions => _documentDeletionActions.Count;

			public int WroteIdentities => _identityActions.Count;

			public void Dispose()
			{
			}

			public void Initialize(DatabaseSmugglerOptions options)
			{
			}

			public IDatabaseSmugglerIndexActions IndexActions()
			{
				return _indexActions;
			}

			public IDatabaseSmugglerDocumentActions DocumentActions()
			{
				return _documentActions;
			}

			public IDatabaseSmugglerTransformerActions TransformerActions()
			{
				return _transformerActions;
			}

			public IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions()
			{
				return _documentDeletionActions;
			}

			public IDatabaseSmugglerIdentityActions IdentityActions()
			{
				return _identityActions;
			}

			public OperationState ModifyOperationState(DatabaseSmugglerOptions options, OperationState state)
			{
				return state;
			}

			private class DatabaseSmugglerCountingIndexActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerIndexActions
			{
				public Task WriteIndexAsync(IndexDefinition index)
				{
					Count++;
					return new CompletedTask();
				}
			}

			private class DatabaseSmugglerCountingIdentityActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerIdentityActions
			{
				public Task WriteIdentityAsync(string name, long value)
				{
					Count++;
					return new CompletedTask();
				}
			}

			private class DatabaseSmugglerCountingDocumentDeletionActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerDocumentDeletionActions
			{
				public Task WriteDocumentDeletionAsync(string key)
				{
					Count++;
					return new CompletedTask();
				}
			}

			private class DatabaseSmugglerCountingTransformerActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerTransformerActions
			{
				public Task WriteTransformerAsync(TransformerDefinition transformer)
				{
					Count++;
					return new CompletedTask();
				}
			}

			private class DatabaseSmugglerCountingDocumentActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerDocumentActions
			{
				public Task WriteDocumentAsync(RavenJObject document)
				{
					Count++;
					return new CompletedTask();
				}
			}

			private class DatabaseSmugglerCountingActionsBase : IDisposable
			{
				public int Count { get; set; }

				public void Dispose()
				{
				}
			}
		}
	}


}