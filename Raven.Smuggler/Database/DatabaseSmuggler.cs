// -----------------------------------------------------------------------
//  <copyright file="Smuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
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
			AsyncHelpers.RunSync(ExecuteAsync);
		}

		public async Task ExecuteAsync()
		{
			using (_source)
			using (_destination)
			{
				_source.Initialize(_options);
				_destination.Initialize(_options);

				var state = GetOperationState(_options, _source, _destination);

				var sources = _source.SupportsMultipleSources
					? _source.Sources
					: new List<IDatabaseSmugglerSource> { _source };

				foreach (var source in sources)
					await ProcessSourceAsync(source, state).ConfigureAwait(false);
			}
		}

		private async Task ProcessSourceAsync(IDatabaseSmugglerSource source, OperationState state)
		{
			var maxEtags = await source
						.FetchCurrentMaxEtagsAsync()
						.ConfigureAwait(false);

			while (true)
			{
				var type = await source
					.GetNextSmuggleTypeAsync()
					.ConfigureAwait(false);

				switch (type)
				{
					case SmuggleType.None:
						return;
					case SmuggleType.Index:
						await new IndexSmuggler(_options, _report, source, _destination)
							.SmuggleAsync(state)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Document:
						await new DocumentSmuggler(_options, _report, source, _destination, maxEtags)
							.SmuggleAsync(state)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Transformer:
						await new TransformerSmuggler(_options, _report, source, _destination)
							.SmuggleAsync(state)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.DocumentDeletion:
						await new DocumentDeletionsSmuggler(_options, _report, source, _destination, maxEtags)
							.SmuggleAsync(state)
							.ConfigureAwait(false);
						continue;
					case SmuggleType.Identity:
						await new IdentitySmuggler(_options, _report, source, _destination)
							.SmuggleAsync(state)
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