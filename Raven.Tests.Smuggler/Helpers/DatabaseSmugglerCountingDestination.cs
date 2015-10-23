using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Smuggler.Database;

namespace Raven.Tests.Smuggler.Helpers
{
	public class DatabaseSmugglerCountingDestination : IDatabaseSmugglerDestination
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

		public bool SupportsOperationState => false;

		public bool SupportsWaitingForIndexing => false;

		public Task InitializeAsync(DatabaseSmugglerOptions options, DatabaseSmugglerNotifications notifications, CancellationToken cancellationToken)
		{
			return new CompletedTask();
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

		public Task<OperationState> LoadOperationStateAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task SaveOperationStateAsync(DatabaseSmugglerOptions options, OperationState state, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task WaitForIndexingAsOfLastWriteAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		private class DatabaseSmugglerCountingIndexActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerIndexActions
		{
			public Task WriteIndexAsync(IndexDefinition index, CancellationToken cancellationToken)
			{
				Count++;
				return new CompletedTask();
			}
		}

		private class DatabaseSmugglerCountingIdentityActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerIdentityActions
		{
			public Task WriteIdentityAsync(string name, long value, CancellationToken cancellationToken)
			{
				Count++;
				return new CompletedTask();
			}
		}

		private class DatabaseSmugglerCountingDocumentDeletionActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerDocumentDeletionActions
		{
			public Task WriteDocumentDeletionAsync(string key, CancellationToken cancellationToken)
			{
				Count++;
				return new CompletedTask();
			}
		}

		private class DatabaseSmugglerCountingTransformerActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerTransformerActions
		{
			public Task WriteTransformerAsync(TransformerDefinition transformer, CancellationToken cancellationToken)
			{
				Count++;
				return new CompletedTask();
			}
		}

		private class DatabaseSmugglerCountingDocumentActions : DatabaseSmugglerCountingActionsBase, IDatabaseSmugglerDocumentActions
		{
			public Task WriteDocumentAsync(RavenJObject document, CancellationToken cancellationToken)
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