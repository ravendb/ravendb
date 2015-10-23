// -----------------------------------------------------------------------
//  <copyright file="TransformerSmuggler.cs" company="Hibernating Rhinos LTD">
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
	internal class TransformerSmuggler : SmugglerBase
	{
		public TransformerSmuggler(DatabaseSmugglerOptions options, DatabaseSmugglerNotifications notifications, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
			: base(options, notifications, source, destination)
		{
		}

		public override async Task SmuggleAsync(DatabaseSmugglerOperationState state, CancellationToken cancellationToken)
		{
			using (var actions = Destination.TransformerActions())
			{
				if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Transformers) == false)
				{
					await Source.SkipTransformersAsync(cancellationToken).ConfigureAwait(false);
					return;
				}

				var count = 0;
				var retries = Source.SupportsRetries ? DatabaseSmuggler.NumberOfRetries : 1;
				var pageSize = Source.SupportsPaging ? Options.BatchSize : int.MaxValue;
				do
				{
					List<TransformerDefinition> transformers;
					try
					{
						transformers = await Source.ReadTransformersAsync(count, pageSize, cancellationToken).ConfigureAwait(false);
					}
					catch (Exception e)
					{
						if (retries-- == 0 && Options.IgnoreErrorsAndContinue)
						{
							Notifications.ShowProgress("Failed getting transformers too much times, stopping the transformer export entirely. Message: {0}", e.Message);
							return;
						}

						if (Options.IgnoreErrorsAndContinue == false)
							throw new SmugglerExportException(e.Message, e);

						Notifications.ShowProgress("Failed fetching transformers. {0} retries remaining. Message: {1}", retries, e.Message);
						continue;
					}

					if (transformers.Count == 0)
					{
						Notifications.ShowProgress("Done with reading transformers, total: {0}", count);
						break;
					}

					count += transformers.Count;
					Notifications.ShowProgress("Reading batch of {0,3} transformers, read so far: {1,10:#,#;;0}", transformers.Count, count);

					foreach (var transformer in transformers)
					{
						try
						{
							if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Transformers))
								await actions.WriteTransformerAsync(transformer, cancellationToken).ConfigureAwait(false);
						}
						catch (Exception e)
						{
							if (Options.IgnoreErrorsAndContinue == false)
								throw new SmugglerExportException(e.Message, e);

							Notifications.ShowProgress("Failed to export transformer {0}. Message: {1}", transformer, e.Message);
						}
					}
				} while (Source.SupportsPaging || Source.SupportsRetries);
			}
		}
	}
}