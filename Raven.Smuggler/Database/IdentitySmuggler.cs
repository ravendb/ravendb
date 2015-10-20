// -----------------------------------------------------------------------
//  <copyright file="IdentitySmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Smuggler.Database
{
	internal class IdentitySmuggler : SmugglerBase
	{
		public IdentitySmuggler(DatabaseSmugglerOptions options, ReportActions report, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
			: base(options, report, source, destination)
		{
		}

		public override async Task SmuggleAsync(OperationState state, CancellationToken cancellationToken)
		{
			using (var actions = Destination.IdentityActions())
			{
				int readCount = 0, filteredCount = 0, writeCount = 0;
				var retries = Source.SupportsRetries ? DatabaseSmuggler.NumberOfRetries : 1;
				do
				{
					List<KeyValuePair<string, long>> identities;
					try
					{
						identities = await Source.ReadIdentitiesAsync(cancellationToken).ConfigureAwait(false);
					}
					catch (Exception e)
					{
						if (retries-- == 0 && Options.IgnoreErrorsAndContinue)
						{
							Report.ShowProgress("Failed getting identities too much times, stopping the identity export entirely. Message: {0}", e.Message);
							return;
						}

						if (Options.IgnoreErrorsAndContinue == false)
							throw new SmugglerExportException(e.Message, e);

						Report.ShowProgress("Failed fetching identities. {0} retries remaining. Message: {1}", retries, e.Message);
						continue;
					}

					readCount += identities.Count;

					Report.ShowProgress("Exported {0} following identities: {1}", identities.Count, string.Join(", ", identities.Select(x => x.Key)));

					var filteredIdentities = identities.Where(x => FilterIdentity(x.Key, Options.OperateOnTypes)).ToList();

					filteredCount += filteredIdentities.Count;

					Report.ShowProgress("After filtering {0} identities need to be exported: {1}", filteredIdentities.Count, string.Join(", ", filteredIdentities.Select(x => x.Key)));

					foreach (var identity in filteredIdentities)
					{
						try
						{
							await actions.WriteIdentityAsync(identity.Key, identity.Value, cancellationToken).ConfigureAwait(false);
							writeCount++;
						}
						catch (Exception e)
						{
							if (Options.IgnoreErrorsAndContinue == false)
								throw new SmugglerExportException(e.Message, e);

							Report.ShowProgress("Failed to export identity {0}. Message: {1}", identity, e.Message);
						}
					}

					break;
				} while (Source.SupportsRetries);

				Report.ShowProgress("IDENTITY. Read: {0}. Filtered: {1}. Wrote: {2}", readCount, readCount - filteredCount, writeCount);
			}
		}

		private static bool FilterIdentity(string indentityName, ItemType operateOnTypes)
		{
			if ("Raven/Etag".Equals(indentityName, StringComparison.InvariantCultureIgnoreCase))
				return false;

			if ("IndexId".Equals(indentityName, StringComparison.InvariantCultureIgnoreCase))
				return false;

			if (operateOnTypes.HasFlag(ItemType.Documents))
				return true;

			return false;
		}
	}
}