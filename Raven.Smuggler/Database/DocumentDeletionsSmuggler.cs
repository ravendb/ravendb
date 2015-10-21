// -----------------------------------------------------------------------
//  <copyright file="DeletionsSmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Smuggler.Database
{
	internal class DocumentDeletionsSmuggler : SmugglerBase
	{
		private readonly LastEtagsInfo _maxEtags;

		public DocumentDeletionsSmuggler(DatabaseSmugglerOptions options, Report report, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination, LastEtagsInfo maxEtags)
			: base(options, report, source, destination)
		{
			_maxEtags = maxEtags;
		}

		public override async Task SmuggleAsync(OperationState state, CancellationToken cancellationToken)
		{
			using (var actions = Destination.DocumentDeletionActions())
			{
				if (Source.SupportsDocumentDeletions == false)
					return;

				var enumerator = await Source
					.ReadDocumentDeletionsAsync(state.LastDocDeleteEtag, _maxEtags.LastDocDeleteEtag.IncrementBy(1), cancellationToken)
					.ConfigureAwait(false);

				while (await enumerator.MoveNextAsync().ConfigureAwait(false))
				{
					await actions
						.WriteDocumentDeletionAsync(enumerator.Current, cancellationToken)
						.ConfigureAwait(false);
				}
			}
		}
	}
}