// -----------------------------------------------------------------------
//  <copyright file="Smuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;

namespace Raven.Smuggler.Database
{
	public class DatabaseSmuggler
	{
		public const int NumberOfRetries = 3;

		private readonly DatabaseSmugglerOptions _options;

		private readonly IDatabaseSmugglerSource _source;

		private readonly IDatabaseSmugglerDestination _destination;

		private readonly ReportActions _report;

		public DatabaseSmuggler(DatabaseSmugglerOptions options, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
		{
			_options = options;
			_source = source;
			_destination = destination;
			_report = new ReportActions();
		}

		public ReportActions Report
		{
			get
			{
				return _report;
			}
		}

		public void Execute()
		{
			AsyncHelpers.RunSync(() => ExecuteAsync(CancellationToken.None));
		}

		public async Task ExecuteAsync(CancellationToken cancellationToken = default(CancellationToken))
		{
			using (_source)
			using (_destination)
			{
				await _source
					.InitializeAsync(_options, cancellationToken)
					.ConfigureAwait(false);

				await _destination
					.InitializeAsync(_options, cancellationToken)
					.ConfigureAwait(false);

				var state = GetOperationState(_options, _source, _destination);

				var sources = _source.SupportsMultipleSources
					? _source.Sources
					: new List<IDatabaseSmugglerSource> { _source };

				Report.ShowProgress("Found {0} sources.", sources.Count);

				foreach (var source in sources)
					await ProcessSourceAsync(source, state, cancellationToken).ConfigureAwait(false);
			}
		}

		private async Task ProcessSourceAsync(IDatabaseSmugglerSource source, OperationState state, CancellationToken cancellationToken)
		{
			if (string.IsNullOrEmpty(source.DisplayName) == false)
				Report.ShowProgress("Processing source: {0}", source.DisplayName);

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
						await new IndexSmuggler(_options, _report, source, _destination)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Document:
						await new DocumentSmuggler(_options, _report, source, _destination, maxEtags)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Transformer:
						await new TransformerSmuggler(_options, _report, source, _destination)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.DocumentDeletion:
						await new DocumentDeletionsSmuggler(_options, _report, source, _destination, maxEtags)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Identity:
						await new IdentitySmuggler(_options, _report, source, _destination)
							.SmuggleAsync(state, cancellationToken)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Attachment:
					case SmuggleType.AttachmentDeletion:
					default:
						throw new NotSupportedException(type.ToString());
				}
			}
		}

		private static OperationState GetOperationState(DatabaseSmugglerOptions options, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
		{
			var state = new OperationState
			{
				LastDocsEtag = options.StartDocsEtag,
				LastDocDeleteEtag = options.StartDocsDeletionEtag,
			};

			state = destination.ModifyOperationState(options, state);

			return state;
		}
	}
}