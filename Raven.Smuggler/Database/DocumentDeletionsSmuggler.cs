// -----------------------------------------------------------------------
//  <copyright file="DeletionsSmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Raven.Abstractions.Smuggler.Data;

namespace Raven.Smuggler.Database
{
	internal class DocumentDeletionsSmuggler : SmugglerBase
	{
		private readonly LastEtagsInfo _maxEtags;

		public DocumentDeletionsSmuggler(DatabaseSmugglerOptions options, ReportActions report, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination, LastEtagsInfo maxEtags)
			: base(options, report, source, destination)
		{
			_maxEtags = maxEtags;
		}

		public override async Task SmuggleAsync(OperationState state)
		{
			using (var actions = Destination.DocumentDeletionActions())
			{
				if (Source.SupportsDocumentDeletions == false)
					return;

				var enumerator = await Source
					.ReadDocumentDeletionsAsync(state.LastDocDeleteEtag, _maxEtags.LastDocDeleteEtag.IncrementBy(1))
					.ConfigureAwait(false);

				while (await enumerator.MoveNextAsync().ConfigureAwait(false))
				{
					await actions
						.WriteDocumentDeletionAsync(enumerator.Current)
						.ConfigureAwait(false);
				}
			}
		}
	}
}