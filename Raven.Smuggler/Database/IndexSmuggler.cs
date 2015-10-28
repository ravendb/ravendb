// -----------------------------------------------------------------------
//  <copyright file="IndexSmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;

namespace Raven.Smuggler.Database
{
	internal class IndexSmuggler : SmugglerBase
	{
		public IndexSmuggler(DatabaseSmugglerOptions options, DatabaseSmugglerNotifications notifications, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
			: base(options, notifications, source, destination)
		{
		}

		public override async Task SmuggleAsync(DatabaseSmugglerOperationState state, CancellationToken cancellationToken)
		{
			using (var actions = Destination.IndexActions())
			{
				if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Indexes) == false)
				{
					await Source.SkipIndexesAsync(cancellationToken).ConfigureAwait(false);
					return;
				}

				var count = 0;
				var retries = Source.SupportsRetries ? DatabaseSmuggler.NumberOfRetries : 1;
				var pageSize = Source.SupportsPaging ? Options.BatchSize : int.MaxValue;
				do
				{
					List<IndexDefinition> indexes;
					try
					{
						indexes = await Source.ReadIndexesAsync(count, pageSize, cancellationToken).ConfigureAwait(false);
					}
					catch (Exception e)
					{
						if (retries-- == 0 && Options.IgnoreErrorsAndContinue)
						{
							Notifications.ShowProgress("Failed getting indexes too much times, stopping the index export entirely. Message: {0}", e.Message);
							return;
						}

						if (Options.IgnoreErrorsAndContinue == false)
							throw new SmugglerException(e.Message, e);

						Notifications.ShowProgress("Failed fetching indexes. {0} retries remaining. Message: {1}", retries, e.Message);
						continue;
					}

					if (indexes.Count == 0)
					{
						Notifications.ShowProgress("Done with reading indexes, total: {0}", count);
						break;
					}

					count += indexes.Count;
					Notifications.ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Count, count);

					foreach (var index in indexes)
					{
						try
						{
							if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Indexes))
								await actions.WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
						}
						catch (Exception e)
						{
							if (Options.IgnoreErrorsAndContinue == false)
								throw new SmugglerException(e.Message, e);

							Notifications.ShowProgress("Failed to export index {0}. Message: {1}", index, e.Message);
						}
					}
				} while (Source.SupportsPaging || Source.SupportsRetries);
            }
		}
	}
}