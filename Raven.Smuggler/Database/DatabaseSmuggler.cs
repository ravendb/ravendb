// -----------------------------------------------------------------------
//  <copyright file="Smuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;

namespace Raven.Smuggler.Database
{
	public class DatabaseSmuggler
	{
		public const int NumberOfRetries = 3;

		internal readonly DatabaseSmugglerOptions _options;

		private readonly IDatabaseSmugglerSource _source;

		private readonly IDatabaseSmugglerDestination _destination;

		private readonly DatabaseSmugglerNotifications _notifications;

		public DatabaseSmuggler(DatabaseSmugglerOptions options, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
		{
			_options = options;
			_source = source;
			_destination = destination;
			_notifications = new DatabaseSmugglerNotifications();
		}

		public DatabaseSmugglerNotifications Notifications
		{
			get
			{
				return _notifications;
			}
		}

		public DatabaseSmugglerOperationState Execute()
		{
			return AsyncHelpers.RunSync(() => ExecuteAsync(CancellationToken.None));
		}

		public async Task<DatabaseSmugglerOperationState> ExecuteAsync(CancellationToken cancellationToken = default(CancellationToken))
		{
			using (_source)
			using (_destination)
			{
				await _source
					.InitializeAsync(_options, cancellationToken)
					.ConfigureAwait(false);

				await _destination
					.InitializeAsync(_options, _notifications, cancellationToken)
					.ConfigureAwait(false);

				var state = await GetOperationStateAsync(_options, _source, _destination, cancellationToken).ConfigureAwait(false);

				var sources = _source.SupportsMultipleSources
					? _source.Sources
					: new List<IDatabaseSmugglerSource> { _source };

				Notifications.ShowProgress("Found {0} sources.", sources.Count);

				foreach (var source in sources)
					await ProcessSourceAsync(source, state, cancellationToken).ConfigureAwait(false);

				return state;
			}
		}

		private async Task ProcessSourceAsync(IDatabaseSmugglerSource source, DatabaseSmugglerOperationState state, CancellationToken cancellationToken)
		{
			if (string.IsNullOrEmpty(source.DisplayName) == false)
				Notifications.ShowProgress("Processing source: {0}", source.DisplayName);

			var maxEtags = await source
						.FetchCurrentMaxEtagsAsync(cancellationToken)
						.ConfigureAwait(false);

			while (true)
			{
				var type = await source
					.GetNextSmuggleTypeAsync(cancellationToken)
					.ConfigureAwait(false);

				switch (type)
				{
					case SmuggleType.None:
						return;
					case SmuggleType.Index:
						await new IndexSmuggler(_options, _notifications, source, _destination)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Document:
						await new DocumentSmuggler(_options, _notifications, source, _destination, maxEtags)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Transformer:
						await new TransformerSmuggler(_options, _notifications, source, _destination)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.DocumentDeletion:
						await new DocumentDeletionsSmuggler(_options, _notifications, source, _destination, maxEtags)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Identity:
						await new IdentitySmuggler(_options, _notifications, source, _destination)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Attachment:
						await new AttachmentSmuggler(_options, _notifications, source, _destination)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.AttachmentDeletion:
						await new AttachmentDeletionsSmuggler(_options, _notifications, source, _destination)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					default:
						throw new NotSupportedException(type.ToString());
				}
			}
		}

		private static async Task<DatabaseSmugglerOperationState> GetOperationStateAsync(DatabaseSmugglerOptions options, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination, CancellationToken cancellationToken)
		{
			DatabaseSmugglerOperationState state = null;

			if (destination.SupportsOperationState)
			{
				state = await destination
					.LoadOperationStateAsync(options, cancellationToken)
					.ConfigureAwait(false);
			}

			if (state == null)
			{
				state = new DatabaseSmugglerOperationState
				{
					LastDocsEtag = options.StartDocsEtag,
					LastDocDeleteEtag = options.StartDocsDeletionEtag,
				};
			}

			return state;
		}
	}
}