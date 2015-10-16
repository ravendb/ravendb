// -----------------------------------------------------------------------
//  <copyright file="DocumentSmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Database.Smuggler;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database
{
	internal class DocumentSmuggler : SmugglerBase
	{
		private readonly LastEtagsInfo _maxEtags;

		private readonly SmugglerJintHelper _patcher;

		public DocumentSmuggler(DatabaseSmugglerOptions options, ReportActions report, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination, LastEtagsInfo maxEtags)
			: base(options, report, source, destination)
		{
			_maxEtags = maxEtags;
			_patcher = new SmugglerJintHelper();
			_patcher.Initialize(options);
		}

		public override async Task SmuggleAsync(OperationState state)
		{
			using (var actions = Destination.DocumentActions())
			{
				var lastEtag = state.LastDocsEtag;
				var maxEtag = _maxEtags.LastDocsEtag;
				var now = SystemTime.UtcNow;
				var totalCount = 0;
				var lastReport = SystemTime.UtcNow;
				var reportInterval = TimeSpan.FromSeconds(2);
				var reachedMaxEtag = false;
				Report.ShowProgress("Exporting Documents");

				do
				{
					var hasDocs = false;
					try
					{
						var maxRecords = Options.Limit - totalCount;
						// TODO [ppekrol] Handle Limit better
						if (maxRecords > 0 && reachedMaxEtag == false)
						{
							var pageSize = Source.SupportsPaging ? Math.Min(Options.BatchSize, maxRecords) : int.MaxValue;

							using (var documents = await Source.ReadDocumentsAsync(lastEtag, pageSize).ConfigureAwait(false))
							{
								while (await documents.MoveNextAsync().ConfigureAwait(false))
								{
									hasDocs = true;
									var document = documents.Current;

									var tempLastEtag = Etag.Parse(document.Value<RavenJObject>("@metadata").Value<string>("@etag"));

									if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
									{
										reachedMaxEtag = true;
										break;
									}

									lastEtag = tempLastEtag;

									if (Options.MatchFilters(document) == false)
										continue;

									if (Options.ShouldExcludeExpired && Options.ExcludeExpired(document, now))
										continue;

									if (Options.ShouldExcludeExpired && Options.ExcludeExpired(document, now))
										continue;

									if (!string.IsNullOrEmpty(Options.TransformScript))
										document = await TransformDocumentAsync(document).ConfigureAwait(false);

									// If document is null after a transform we skip it. 
									if (document == null)
										continue;

									var metadata = document["@metadata"] as RavenJObject;
									if (metadata != null)
									{
										if (Options.SkipConflicted && metadata.ContainsKey(Constants.RavenReplicationConflictDocument))
											continue;

										if (Options.StripReplicationInformation)
											document["@metadata"] = SmugglerHelper.StripReplicationInformationFromMetadata(metadata);

										if (Options.ShouldDisableVersioningBundle)
											document["@metadata"] = SmugglerHelper.DisableVersioning(metadata);

										document["@metadata"] = SmugglerHelper.HandleConflictDocuments(metadata);
									}

									try
									{
										await actions.WriteDocumentAsync(document).ConfigureAwait(false);
									}
									catch (Exception e)
									{
										if (Options.IgnoreErrorsAndContinue == false)
											throw;

										Report.ShowProgress("EXPORT of a document {0} failed. Message: {1}", document, e.Message);
									}

									totalCount++;

									if (totalCount % 1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
									{
										Report.ShowProgress("Exported {0} documents", totalCount);
										lastReport = SystemTime.UtcNow;
									}
								}
							}

							if (hasDocs)
								continue;

							if (Source.SupportsGettingStatistics)
							{
								// The server can filter all the results. In this case, we need to try to go over with the next batch.
								// Note that if the ETag' server restarts number is not the same, this won't guard against an infinite loop.
								// (This code provides support for legacy RavenDB version: 1.0)
								var databaseStatistics = await Source.GetStatisticsAsync().ConfigureAwait(false);
								var lastEtagComparable = new ComparableByteArray(lastEtag);
								if (lastEtagComparable.CompareTo(databaseStatistics.LastDocEtag) < 0)
								{
									lastEtag = EtagUtil.Increment(lastEtag, pageSize);
									if (lastEtag.CompareTo(databaseStatistics.LastDocEtag) >= 0)
									{
										lastEtag = databaseStatistics.LastDocEtag;
									}
									Report.ShowProgress("Got no results but didn't get to the last doc etag, trying from: {0}", lastEtag);
									continue;
								}
							}
						}

						if (Source.SupportsReadingSingleDocuments)
						{
							// Load HiLo documents for selected collections
							foreach (var filter in Options.Filters)
							{
								if (string.Equals(filter.Path, "@metadata.Raven-Entity-Name", StringComparison.OrdinalIgnoreCase) == false)
									continue;

								foreach (var collectionName in filter.Values)
								{
									var doc = await Source
														.ReadDocumentAsync("Raven/Hilo/" + collectionName)
														.ConfigureAwait(false);

									if (doc == null)
										continue;

									await actions.WriteDocumentAsync(doc).ConfigureAwait(false);
									totalCount++;
								}
							}
						}

						Report.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
						state.LastDocsEtag = lastEtag;
					}
					catch (Exception e)
					{
						Report.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
						Report.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
						throw new SmugglerExportException(e.Message, e)
						{
							LastEtag = lastEtag,
						};
					}
				} while (Source.SupportsPaging);
			}
		}

		private Task<RavenJObject> TransformDocumentAsync(RavenJObject document)
		{
			return new CompletedTask<RavenJObject>(_patcher.Transform(document));
		}
	}
}