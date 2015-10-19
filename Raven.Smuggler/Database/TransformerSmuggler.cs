// -----------------------------------------------------------------------
//  <copyright file="TransformerSmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Smuggler.Database
{
	internal class TransformerSmuggler : SmugglerBase
	{
		public TransformerSmuggler(DatabaseSmugglerOptions options, ReportActions report, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
			: base(options, report, source, destination)
		{
		}

		public override async Task SmuggleAsync(OperationState state)
		{
			using (var actions = Destination.TransformerActions())
			{
				var count = 0;
				var retries = Source.SupportsRetries ? DatabaseSmuggler.NumberOfRetries : 1;
				var pageSize = Source.SupportsPaging ? Options.BatchSize : int.MaxValue;
				do
				{
					List<TransformerDefinition> transformers;
					try
					{
						transformers = await Source.ReadTransformersAsync(count, pageSize).ConfigureAwait(false);
					}
					catch (Exception e)
					{
						if (retries-- == 0 && Options.IgnoreErrorsAndContinue)
						{
							Report.ShowProgress("Failed getting transformers too much times, stopping the transformer export entirely. Message: {0}", e.Message);
							return;
						}

						if (Options.IgnoreErrorsAndContinue == false)
							throw new SmugglerExportException(e.Message, e);

						Report.ShowProgress("Failed fetching transformers. {0} retries remaining. Message: {1}", retries, e.Message);
						continue;
					}

					if (transformers.Count == 0)
					{
						Report.ShowProgress("Done with reading transformers, total: {0}", count);
						break;
					}

					count += transformers.Count;
					Report.ShowProgress("Reading batch of {0,3} transformers, read so far: {1,10:#,#;;0}", transformers.Count, count);

					foreach (var transformer in transformers)
					{
						try
						{
							if (Options.OperateOnTypes.HasFlag(ItemType.Transformers))
								await actions.WriteTransformerAsync(transformer).ConfigureAwait(false);
						}
						catch (Exception e)
						{
							if (Options.IgnoreErrorsAndContinue == false)
								throw new SmugglerExportException(e.Message, e);

							Report.ShowProgress("Failed to export transformer {0}. Message: {1}", transformer, e.Message);
						}
					}
				} while (Source.SupportsPaging || Source.SupportsRetries);
			}
		}
	}
}