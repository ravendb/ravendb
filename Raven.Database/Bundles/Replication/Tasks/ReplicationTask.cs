//-----------------------------------------------------------------------
// <copyright file="ReplicationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Data;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Config.Retriever;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Json.Linq;
using System.Globalization;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Bundles.Replication.Tasks
{
	using Database.Indexing;

	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(IStartupTask))]
	public class ReplicationTask : IStartupTask, IDisposable
	{
		public bool IsRunning { get; private set; }

		private volatile bool shouldPause;

		public const int SystemDocsLimitForRemoteEtagUpdate = 15;
		public const int DestinationDocsLimitForRemoteEtagUpdate = 15;

		public readonly ConcurrentQueue<Task> activeTasks = new ConcurrentQueue<Task>();

		private readonly ConcurrentDictionary<string, DestinationStats> destinationStats =
			new ConcurrentDictionary<string, DestinationStats>(StringComparer.OrdinalIgnoreCase);

		private DocumentDatabase docDb;
		private readonly static ILog log = LogManager.GetCurrentClassLogger();
		private bool firstTimeFoundNoReplicationDocument = true;
		private bool wrongReplicationSourceAlertSent = false;
		private readonly ConcurrentDictionary<string, SemaphoreSlim> activeReplicationTasks = new ConcurrentDictionary<string, SemaphoreSlim>();

		private TimeSpan _replicationFrequency;
		private TimeSpan _lastQueriedFrequency;
		private Timer _indexReplicationTaskTimer;
		private Timer _lastQueriedTaskTimer;
		private object _indexReplicationTaskLock = new object();
		private object _lastQueriedTaskLock = new object();

		private readonly ConcurrentDictionary<string, DateTime> destinationAlertSent = new ConcurrentDictionary<string, DateTime>(); 

		public ConcurrentDictionary<string, DestinationStats> DestinationStats
		{
			get { return destinationStats; }
		}

		public ConcurrentDictionary<string, DateTime> Heartbeats
		{
			get { return heartbeatDictionary; }
		}

		private int replicationAttempts;
		private int workCounter;
		private HttpRavenRequestFactory httpRavenRequestFactory;
		private HttpRavenRequestFactory nonBufferedHttpRavenRequestFactory;

		private IndependentBatchSizeAutoTuner autoTuner;
		internal readonly ConcurrentDictionary<string, PrefetchingBehavior> prefetchingBehaviors = new ConcurrentDictionary<string, PrefetchingBehavior>();

		public void Execute(DocumentDatabase database)
		{
			docDb = database;
			var replicationRequestTimeoutInMs = docDb.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds;

			autoTuner = new IndependentBatchSizeAutoTuner(docDb.WorkContext, PrefetchingUser.Replicator);
			httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = replicationRequestTimeoutInMs };
			nonBufferedHttpRavenRequestFactory = new HttpRavenRequestFactory
			{
				RequestTimeoutInMs = replicationRequestTimeoutInMs,
				AllowWriteStreamBuffering = false
			};

			var task = new Task(Execute, TaskCreationOptions.LongRunning);
			var disposableAction = new DisposableAction(task.Wait);
			// make sure that the doc db waits for the replication task shutdown
			docDb.ExtensionsState.GetOrAdd(Guid.NewGuid().ToString(), s => disposableAction);

			_replicationFrequency = TimeSpan.FromSeconds(database.Configuration.IndexAndTransformerReplicationLatencyInSec); //by default 10 min
			_lastQueriedFrequency = TimeSpan.FromSeconds(database.Configuration.TimeToWaitBeforeRunningIdleIndexes.TotalSeconds / 2);

			_indexReplicationTaskTimer = database.TimerManager.NewTimer(ReplicateIndexesAndTransformersTask, TimeSpan.Zero, _replicationFrequency);
			_lastQueriedTaskTimer = database.TimerManager.NewTimer(SendLastQueriedTask, TimeSpan.Zero, _lastQueriedFrequency);


			task.Start();
		}

		public void Pause()
		{
			shouldPause = true;
		}

		public void Continue()
		{
			shouldPause = false;
		}

		private void Execute()
		{
			using (LogContext.WithDatabase(docDb.Name))
			{
				var name = GetType().Name;

				var timeToWaitInMinutes = TimeSpan.FromMinutes(5);
				bool runningBecauseOfDataModifications = false;
				var context = docDb.WorkContext;
				NotifySiblings();
				while (context.DoWork)
				{
					IsRunning = !shouldPause;

					if (IsRunning)
					{
						try
						{
							using (docDb.DisableAllTriggersForCurrentThread())
							{
								var destinations = GetReplicationDestinations();

								if (destinations.Length == 0)
								{
									WarnIfNoReplicationTargetsWereFound();
								}
								else
								{
									var currentReplicationAttempts = Interlocked.Increment(ref replicationAttempts);

									var copyOfrunningBecauseOfDataModifications = runningBecauseOfDataModifications;
									var destinationForReplication = destinations.Where(
										dest =>
										{
											if (copyOfrunningBecauseOfDataModifications == false) return true;
											return IsNotFailing(dest, currentReplicationAttempts);
										}).ToList();

									CleanupPrefetchingBehaviors(destinations.Select(x => x.ConnectionStringOptions.Url),
										destinations.Except(destinationForReplication).Select(x => x.ConnectionStringOptions.Url));

									var startedTasks = new List<Task>();

									foreach (var dest in destinationForReplication)
									{
										var destination = dest;
										var holder = activeReplicationTasks.GetOrAdd(destination.ConnectionStringOptions.Url, s => new SemaphoreSlim(1));
										if (holder.Wait(0) == false)
											continue;

										var replicationTask = Task.Factory.StartNew(
											() =>
											{
												using (LogContext.WithDatabase(docDb.Name))
												{
													try
													{
														if (ReplicateTo(destination)) docDb.WorkContext.NotifyAboutWork();
													}
													catch (Exception e)
													{
														log.ErrorException("Could not replicate to " + destination, e);
													}
												}
											});

										startedTasks.Add(replicationTask);

										activeTasks.Enqueue(replicationTask);
										replicationTask.ContinueWith(
											_ =>
											{
												// here we purge all the completed tasks at the head of the queue
												Task task;
												while (activeTasks.TryPeek(out task))
												{
													if (!task.IsCompleted && !task.IsCanceled && !task.IsFaulted) break;
													activeTasks.TryDequeue(out task); // remove it from end
												}
											});
									}

									Task.WhenAll(startedTasks.ToArray()).ContinueWith(
										t =>
										{
											if (destinationStats.Count == 0)
												return;

											foreach (var stats in destinationStats.Where(stats => stats.Value.LastReplicatedEtag != null))
											{
												PrefetchingBehavior prefetchingBehavior;

												if (prefetchingBehaviors.TryGetValue(stats.Key, out prefetchingBehavior))
												{
													prefetchingBehavior.CleanupDocuments(stats.Value.LastReplicatedEtag);
												}
											}
										}).AssertNotFailed();
								}
							}
						}
						catch (Exception e)
						{
							log.ErrorException("Failed to perform replication", e);
						}
					}

					runningBecauseOfDataModifications = context.WaitForWork(timeToWaitInMinutes, ref workCounter, name);
					timeToWaitInMinutes = runningBecauseOfDataModifications
											? TimeSpan.FromSeconds(30)
											: TimeSpan.FromMinutes(5);
				}

				IsRunning = false;
			}
		}

		private void CleanupPrefetchingBehaviors(IEnumerable<string> allDestinations, IEnumerable<string> failingDestinations)
		{
			PrefetchingBehavior prefetchingBehaviorToDispose;

			// remove prefetching behaviors for non-existing destinations
			foreach (var removedDestination in prefetchingBehaviors.Keys.Except(allDestinations))
			{
				if (prefetchingBehaviors.TryRemove(removedDestination, out prefetchingBehaviorToDispose))
				{
					prefetchingBehaviorToDispose.Dispose();
				}
			}

			// also remove prefetchers if the destination is failing for a long time
			foreach (var failingDestination in failingDestinations)
			{
				DestinationStats stats;
				if (prefetchingBehaviors.ContainsKey(failingDestination) == false || destinationStats.TryGetValue(failingDestination, out stats) == false)
					continue;

				if (stats.FirstFailureInCycleTimestamp != null && stats.LastFailureTimestamp != null &&
					stats.LastFailureTimestamp - stats.FirstFailureInCycleTimestamp >= TimeSpan.FromMinutes(3))
				{
					if (prefetchingBehaviors.TryRemove(failingDestination, out prefetchingBehaviorToDispose))
					{
						prefetchingBehaviorToDispose.Dispose();
					}
				}
			}
		}

		private void NotifySiblings()
		{
			var notifications = new BlockingCollection<RavenConnectionStringOptions>();

			Task.Factory.StartNew(() => NotifySibling(notifications));

			int skip = 0;
			var replicationDestinations = GetReplicationDestinations();
			foreach (var replicationDestination in replicationDestinations)
			{
				notifications.TryAdd(replicationDestination.ConnectionStringOptions, 15 * 1000);
			}

			while (true)
			{
				int nextPageStart = skip; // will trigger rapid pagination
				var docs = docDb.Documents.GetDocumentsWithIdStartingWith(Constants.RavenReplicationSourcesBasePath, null, null, skip, 128, CancellationToken.None, ref nextPageStart);
				if (docs.Length == 0)
				{
					notifications.TryAdd(null, 15 * 1000); // marker to stop notify this
					return;
				}

				skip += docs.Length;

				foreach (RavenJObject doc in docs)
				{
					var sourceReplicationInformation = doc.JsonDeserialization<SourceReplicationInformation>();
					if (string.IsNullOrEmpty(sourceReplicationInformation.Source))
						continue;

					var match = replicationDestinations.FirstOrDefault(x =>
														   string.Equals(x.ConnectionStringOptions.Url,
																		 sourceReplicationInformation.Source,
																		 StringComparison.OrdinalIgnoreCase));

					if (match != null)
					{
						notifications.TryAdd(match.ConnectionStringOptions, 15 * 1000);
					}
					else
					{
						notifications.TryAdd(new RavenConnectionStringOptions
						{
							Url = sourceReplicationInformation.Source
						}, 15 * 1000);
					}
				}
			}
		}

		private void NotifySibling(BlockingCollection<RavenConnectionStringOptions> collection)
		{
			using (LogContext.WithDatabase(docDb.Name))
				while (true)
				{
					RavenConnectionStringOptions connectionStringOptions;
					try
					{
						collection.TryTake(out connectionStringOptions, 15 * 1000, docDb.WorkContext.CancellationToken);
						if (connectionStringOptions == null)
							return;
					}
					catch (Exception e)
					{
						log.ErrorException("Could not get connection string options to notify sibling servers about restart", e);
						return;
					}
					try
					{
						var url = connectionStringOptions.Url + "/replication/heartbeat?from=" + UrlEncodedServerUrl() + "&dbid=" + docDb.TransactionalStorage.Id;
						var request = httpRavenRequestFactory.Create(url, "POST", connectionStringOptions);
						request.WebRequest.ContentLength = 0;
						request.ExecuteRequest();
					}
					catch (Exception e)
					{
						log.WarnException("Could not notify " + connectionStringOptions.Url + " about sibling server being up & running", e);
					}
				}
		}

		private bool IsNotFailing(ReplicationStrategy dest, int currentReplicationAttempts)
		{
			var jsonDocument = docDb.Documents.Get(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(dest.ConnectionStringOptions.Url), null);
			if (jsonDocument == null)
				return true;
			var failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
			if (failureInformation.FailureCount > 1000)
			{
				var shouldReplicateTo = currentReplicationAttempts % 10 == 0;
				log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
					dest, failureInformation.FailureCount, shouldReplicateTo == false);
				return shouldReplicateTo;
			}
			if (failureInformation.FailureCount > 100)
			{
				var shouldReplicateTo = currentReplicationAttempts % 5 == 0;
				log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
					dest, failureInformation.FailureCount, shouldReplicateTo == false);
				return shouldReplicateTo;
			}
			if (failureInformation.FailureCount > 10)
			{
				var shouldReplicateTo = currentReplicationAttempts % 2 == 0;
				log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
					dest, failureInformation.FailureCount, shouldReplicateTo == false);
				return shouldReplicateTo;
			}
			return true;
		}

		public static string EscapeDestinationName(string url)
		{
			return Uri.EscapeDataString(url.Replace("https://", "").Replace("http://", "").Replace("/", "").Replace(":", ""));
		}

		private void WarnIfNoReplicationTargetsWereFound()
		{
			if (firstTimeFoundNoReplicationDocument)
			{
				firstTimeFoundNoReplicationDocument = false;
				log.Warn("Replication bundle is installed, but there is no destination in 'Raven/Replication/Destinations'.\r\nReplication results in NO-OP");
			}
		}

		private bool ReplicateTo(ReplicationStrategy destination)
		{
			try
			{
				if (docDb.Disposed)
					return false;

				using (docDb.DisableAllTriggersForCurrentThread())
				using (var stats = new ReplicationStatisticsRecorder(destination, destinationStats))
				{
					SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource;
					using (var scope = stats.StartRecording("Destination"))
					{
						try
						{
							destinationsReplicationInformationForSource = GetLastReplicatedEtagFrom(destination);
							if (destinationsReplicationInformationForSource == null)
							{
								destinationsReplicationInformationForSource = GetLastReplicatedEtagFrom(destination);

								if (destinationsReplicationInformationForSource == null)
									return false;
							}

							scope.Record(RavenJObject.FromObject(destinationsReplicationInformationForSource));

							if (destinationsReplicationInformationForSource.LastDocumentEtag == Etag.InvalidEtag && destinationsReplicationInformationForSource.LastAttachmentEtag == Etag.InvalidEtag)
							{
								DateTime lastSent;
								if (destinationAlertSent.TryGetValue(destination.ConnectionStringOptions.Url, out lastSent) && (SystemTime.UtcNow - lastSent).TotalMinutes < 1)
									return false;

								var lastModifiedDate = destinationsReplicationInformationForSource.LastModified.HasValue ? destinationsReplicationInformationForSource.LastModified.Value.ToLocalTime() : DateTime.MinValue;

								docDb.AddAlert(new Alert
								{
									AlertLevel = AlertLevel.Error,
									CreatedAt = SystemTime.UtcNow,
									Message = string.Format(@"Destination server is forbidding replication due to a possibility of having multiple instances with same DatabaseId replicating to it. After 10 minutes from '{2}' another instance will start replicating. Destination Url: {0}. DatabaseId: {1}. Current source: {3}. Stored source on destination: {4}.", destination.ConnectionStringOptions.Url, docDb.TransactionalStorage.Id, lastModifiedDate, docDb.ServerUrl, destinationsReplicationInformationForSource.Source),
									Title = string.Format("Replication error. Multiple databases replicating at the same time with same DatabaseId ('{0}') detected.", docDb.TransactionalStorage.Id),
									UniqueKey = "Replication to " + destination.ConnectionStringOptions.Url + " errored. Wrong DatabaseId: " + docDb.TransactionalStorage.Id
								});

								destinationAlertSent.AddOrUpdate(destination.ConnectionStringOptions.Url, SystemTime.UtcNow, (_, __) => SystemTime.UtcNow);

								return false;
							}
						}
						catch (Exception e)
						{
							scope.RecordError(e);
							log.WarnException("Failed to replicate to: " + destination, e);
							return false;
						}
					}

					bool? replicated = null;

					int replicatedDocuments;

					using (var scope = stats.StartRecording("Documents"))
					{
						switch (ReplicateDocuments(destination, destinationsReplicationInformationForSource, scope, out replicatedDocuments))
						{
							case true:
								replicated = true;
								break;
							case false:
								return false;
						}
					}

					using (var scope = stats.StartRecording("Attachments"))
					{
						switch (ReplicateAttachments(destination, destinationsReplicationInformationForSource, scope))
						{
							case true:
								replicated = true;
								break;
							case false:
								return false;
						}
					}

					var elapsedMicroseconds = (long)(stats.ElapsedTime.Ticks * SystemTime.MicroSecPerTick);
					docDb.WorkContext.MetricsCounters.GetReplicationDurationHistogram(destination).Update(elapsedMicroseconds);
					UpdateReplicationPerformance(destination, stats.Started, stats.ElapsedTime, replicatedDocuments);

					return replicated ?? false;
				}
			}
			finally
			{
				var holder = activeReplicationTasks.GetOrAdd(destination.ConnectionStringOptions.Url, s => new SemaphoreSlim(0, 1));
				holder.Release();
			}
		}

		private void UpdateReplicationPerformance(ReplicationStrategy destination, DateTime startTime, TimeSpan elapsed, int batchSize)
		{
			if (batchSize > 0)
			{
				var queue = docDb.WorkContext.MetricsCounters.GetReplicationPerformanceStats(destination);
				queue.Enqueue(new ReplicationPerformanceStats
				{
					Duration = elapsed,
					Started = startTime,
					BatchSize = batchSize
				});

				while (queue.Count() > 25)
				{
					ReplicationPerformanceStats _;
					queue.TryDequeue(out _);
				}
			}
		}


		[Obsolete("Use RavenFS instead.")]
		private bool? ReplicateAttachments(ReplicationStrategy destination, SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource, ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope recorder)
		{
			Tuple<RavenJArray, Etag> tuple;
			RavenJArray attachments;

			using (var scope = recorder.StartRecording("Get"))
			{
				tuple = GetAttachments(destinationsReplicationInformationForSource, destination, scope);
				attachments = tuple.Item1;

				if (attachments == null || attachments.Length == 0)
				{
					if (tuple.Item2 != destinationsReplicationInformationForSource.LastAttachmentEtag)
					{
						SetLastReplicatedEtagForServer(destination, lastAttachmentEtag: tuple.Item2);
					}
					return null;
				}
			}

			using (var scope = recorder.StartRecording("Send"))
			{
				string lastError;
				if (TryReplicationAttachments(destination, attachments, out lastError) == false) // failed to replicate, start error handling strategy
				{
					if (IsFirstFailure(destination.ConnectionStringOptions.Url))
					{
						log.Info("This is the first failure for {0}, assuming transient failure and trying again", destination);
						if (TryReplicationAttachments(destination, attachments, out lastError)) // success on second fail
						{
							RecordSuccess(destination.ConnectionStringOptions.Url, lastReplicatedEtag: tuple.Item2);
							return true;
						}
					}

					scope.RecordError(lastError);
					RecordFailure(destination.ConnectionStringOptions.Url, lastError);
					return false;
				}
			}

			RecordSuccess(destination.ConnectionStringOptions.Url,
				lastReplicatedEtag: tuple.Item2);

			return true;
		}

		private bool? ReplicateDocuments(ReplicationStrategy destination, SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource, ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope recorder, out int replicatedDocuments)
		{
			replicatedDocuments = 0;
			JsonDocumentsToReplicate documentsToReplicate = null;
			Stopwatch sp = Stopwatch.StartNew();
			IDisposable removeBatch = null;

			var prefetchingBehavior = prefetchingBehaviors.GetOrAdd(destination.ConnectionStringOptions.Url,
				x => docDb.Prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Replicator, autoTuner));


			prefetchingBehavior.AdditionalInfo = string.Format("For destination: {0}. Last replicated etag: {1}", destination.ConnectionStringOptions.Url, destinationsReplicationInformationForSource.LastDocumentEtag);

			try
			{
				using (var scope = recorder.StartRecording("Get"))
				{
					documentsToReplicate = GetJsonDocuments(destinationsReplicationInformationForSource, destination, prefetchingBehavior, scope);
					if (documentsToReplicate.Documents == null || documentsToReplicate.Documents.Length == 0)
					{
						if (documentsToReplicate.LastEtag != destinationsReplicationInformationForSource.LastDocumentEtag)
						{
							// we don't notify remote server about updates to system docs, see: RavenDB-715
							if (documentsToReplicate.CountOfFilteredDocumentsWhichAreSystemDocuments == 0
								|| documentsToReplicate.CountOfFilteredDocumentsWhichAreSystemDocuments > SystemDocsLimitForRemoteEtagUpdate
								|| documentsToReplicate.CountOfFilteredDocumentsWhichOriginFromDestination > DestinationDocsLimitForRemoteEtagUpdate) // see RavenDB-1555
							{
								using (scope.StartRecording("Notify"))
								{
									SetLastReplicatedEtagForServer(destination, lastDocEtag: documentsToReplicate.LastEtag);
									scope.Record(new RavenJObject
								             {
									             { "LastDocEtag", documentsToReplicate.LastEtag.ToString() }
								             });
								}
							}
						}
						RecordLastEtagChecked(destination.ConnectionStringOptions.Url, documentsToReplicate.LastEtag);
						return null;
					}
				}

				// if the db is idling in all respect except sending out replication, let us keep it that way.
				docDb.WorkContext.UpdateFoundWork();

				removeBatch = prefetchingBehavior.UpdateCurrentlyUsedBatches(documentsToReplicate.LoadedDocs);

				using (var scope = recorder.StartRecording("Send"))
				{
					string lastError;
					if (TryReplicationDocuments(destination, documentsToReplicate.Documents, out lastError) == false) // failed to replicate, start error handling strategy
					{
						if (IsFirstFailure(destination.ConnectionStringOptions.Url))
						{
							log.Info(
								"This is the first failure for {0}, assuming transient failure and trying again",
								destination);
							if (TryReplicationDocuments(destination, documentsToReplicate.Documents, out lastError)) // success on second fail
							{
								RecordSuccess(destination.ConnectionStringOptions.Url, documentsToReplicate.LastEtag, documentsToReplicate.LastLastModified);
								return true;
							}
						}
						// if we had an error sending to this endpoint, it might be because we are sending too much data, or because
						// the request timed out. This will let us know that the next time we try, we'll use just the initial doc counts
						// and we'll be much more conservative with increasing the sizes
						prefetchingBehavior.OutOfMemoryExceptionHappened();
						scope.RecordError(lastError);
						RecordFailure(destination.ConnectionStringOptions.Url, lastError);
						return false;
					}
				}
			}
			finally
			{
				if (documentsToReplicate != null && documentsToReplicate.LoadedDocs != null)
				{
					prefetchingBehavior.UpdateAutoThrottler(documentsToReplicate.LoadedDocs, sp.Elapsed);
					replicatedDocuments = documentsToReplicate.LoadedDocs.Count;
				}

				if (removeBatch != null)
					removeBatch.Dispose();
			}

			RecordSuccess(destination.ConnectionStringOptions.Url, documentsToReplicate.LastEtag, documentsToReplicate.LastLastModified);
			return true;
		}

		private void SetLastReplicatedEtagForServer(ReplicationStrategy destination, Etag lastDocEtag = null, Etag lastAttachmentEtag = null)
		{
			try
			{
				var url = destination.ConnectionStringOptions.Url + "/replication/lastEtag?from=" + UrlEncodedServerUrl() +
						  "&dbid=" + docDb.TransactionalStorage.Id;
				if (lastDocEtag != null)
					url += "&docEtag=" + lastDocEtag;
				if (lastAttachmentEtag != null)
					url += "&attachmentEtag=" + lastAttachmentEtag;

				var request = httpRavenRequestFactory.Create(url, "PUT", destination.ConnectionStringOptions);
				request.Write(new byte[0]);
				request.ExecuteRequest();
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound))
					log.WarnException("Replication is not enabled on: " + destination, e);
				else
					log.WarnException("Failed to contact replication destination: " + destination, e);
			}
			catch (Exception e)
			{
				log.WarnException("Failed to contact replication destination: " + destination, e);
			}
		}

		private void RecordFailure(string url, string lastError)
		{
			var stats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
			Interlocked.Increment(ref stats.FailureCountInternal);
			stats.LastFailureTimestamp = SystemTime.UtcNow;

			if (stats.FirstFailureInCycleTimestamp == null)
				stats.FirstFailureInCycleTimestamp = SystemTime.UtcNow;

			if (string.IsNullOrWhiteSpace(lastError) == false)
				stats.LastError = lastError;

			var jsonDocument = docDb.Documents.Get(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(url), null);
			var failureInformation = new DestinationFailureInformation { Destination = url };
			if (jsonDocument != null)
			{
				failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
			}
			failureInformation.FailureCount += 1;
			docDb.Documents.Put(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(url), null,
					  RavenJObject.FromObject(failureInformation), new RavenJObject(), null);
		}

		private void RecordLastEtagChecked(string url, Etag lastEtagChecked)
		{
			var stats = destinationStats.GetOrDefault(url, new DestinationStats { Url = url });
			stats.LastEtagCheckedForReplication = lastEtagChecked;
		}

		private void RecordSuccess(string url,
			Etag lastReplicatedEtag = null, DateTime? lastReplicatedLastModified = null,
			DateTime? lastHeartbeatReceived = null, string lastError = null)
		{
			var stats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
			Interlocked.Exchange(ref stats.FailureCountInternal, 0);
			stats.LastSuccessTimestamp = SystemTime.UtcNow;
			stats.FirstFailureInCycleTimestamp = null;

			if (lastReplicatedEtag != null)
			{
				stats.LastEtagCheckedForReplication = lastReplicatedEtag;
				stats.LastReplicatedEtag = lastReplicatedEtag;
			}

			if (lastReplicatedLastModified.HasValue)
				stats.LastReplicatedLastModified = lastReplicatedLastModified;

			if (lastHeartbeatReceived.HasValue)
				stats.LastHeartbeatReceived = lastHeartbeatReceived;

			if (!string.IsNullOrWhiteSpace(lastError))
				stats.LastError = lastError;

			docDb.Documents.Delete(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(url), null, null);
		}

		private bool IsFirstFailure(string url)
		{
			var destStats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
			return destStats.FailureCount == 0;
		}

		[Obsolete("Use RavenFS instead.")]
		private bool TryReplicationAttachments(ReplicationStrategy destination, RavenJArray jsonAttachments, out string errorMessage)
		{
			try
			{
				var url = destination.ConnectionStringOptions.Url + "/replication/replicateAttachments?from=" +
						  UrlEncodedServerUrl() + "&dbid=" + docDb.TransactionalStorage.Id;

				var sp = Stopwatch.StartNew();
				using (HttpRavenRequestFactory.Expect100Continue(destination.ConnectionStringOptions.Url))
				{
					var request = nonBufferedHttpRavenRequestFactory.Create(url, "POST", destination.ConnectionStringOptions);

					request.WriteBson(jsonAttachments);
					request.ExecuteRequest(docDb.WorkContext.CancellationToken);
					log.Info("Replicated {0} attachments to {1} in {2:#,#;;0} ms", jsonAttachments.Length, destination, sp.ElapsedMilliseconds);
					errorMessage = "";
					return true;
				}
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null)
				{
					using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
					{
						var error = streamReader.ReadToEnd();
						try
						{
							var ravenJObject = RavenJObject.Parse(error);
							log.WarnException("Replication to " + destination + " had failed\r\n" + ravenJObject.Value<string>("Error"), e);
							errorMessage = error;
							return false;
						}
						catch (Exception)
						{
						}

						log.WarnException("Replication to " + destination + " had failed\r\n" + error, e);
						errorMessage = error;
					}
				}
				else
				{
					log.WarnException("Replication to " + destination + " had failed", e);
					errorMessage = e.Message;
				}
				return false;
			}
			catch (Exception e)
			{
				log.WarnException("Replication to " + destination + " had failed", e);
				errorMessage = e.Message;
				return false;
			}
		}

		private bool TryReplicationDocuments(ReplicationStrategy destination, RavenJArray jsonDocuments, out string lastError)
		{
			try
			{
				log.Debug("Starting to replicate {0} documents to {1}", jsonDocuments.Length, destination);
				var url = destination.ConnectionStringOptions.Url + "/replication/replicateDocs?from=" + UrlEncodedServerUrl()
						  + "&dbid=" + docDb.TransactionalStorage.Id +
						  "&count=" + jsonDocuments.Length;

				var sp = Stopwatch.StartNew();

				using (HttpRavenRequestFactory.Expect100Continue(destination.ConnectionStringOptions.Url))
				{
					var request = nonBufferedHttpRavenRequestFactory.Create(url, "POST", destination.ConnectionStringOptions);
					request.Write(jsonDocuments);
					request.ExecuteRequest(docDb.WorkContext.CancellationToken);

					log.Info("Replicated {0} documents to {1} in {2:#,#;;0} ms", jsonDocuments.Length, destination, sp.ElapsedMilliseconds);
					lastError = "";
					return true;
				}
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null)
				{
					var responseStream = response.GetResponseStream();
					if (responseStream != null)
					{
						using (var streamReader = new StreamReader(responseStream))
						{
							var error = streamReader.ReadToEnd();
							log.WarnException("Replication to " + destination + " had failed\r\n" + error, e);
						}
					}
					else
					{
						log.WarnException("Replication to " + destination + " had failed", e);
					}
				}
				else
				{
					log.WarnException("Replication to " + destination + " had failed", e);
				}
				lastError = e.Message;
				return false;
			}
			catch (Exception e)
			{
				log.WarnException("Replication to " + destination + " had failed", e);
				lastError = e.Message;
				return false;
			}
		}


		public void SendLastQueriedTask(object state)
		{
			if (docDb.Disposed)
				return;
			if (Monitor.TryEnter(_lastQueriedTaskLock) == false)
				return;
			try
			{

				var relevantIndexLastQueries = new Dictionary<string, DateTime>();
				var relevantIndexes = docDb.Statistics.Indexes.Where(indexStats => indexStats.IsInvalidIndex == false &&
																				   indexStats.Priority != IndexingPriority.Error &&
																				   indexStats.Priority != IndexingPriority.Disabled &&
																				   indexStats.LastQueryTimestamp.HasValue);
				foreach (var relevantIndex in relevantIndexes)
				{
					relevantIndexLastQueries[relevantIndex.Name] = relevantIndex.LastQueryTimestamp.GetValueOrDefault();
				}

				if (relevantIndexLastQueries.Count == 0)
					return;

				var destinations = GetReplicationDestinations(x => x.SkipIndexReplication == false);
				foreach (var destination in destinations)
				{
					try
					{
						string url = destination.ConnectionStringOptions.Url + "/indexes/last-queried";

						var replicationRequest = nonBufferedHttpRavenRequestFactory.Create(url, "POST", destination.ConnectionStringOptions);
						replicationRequest.Write(RavenJObject.FromObject(relevantIndexLastQueries));
						replicationRequest.ExecuteRequest();
					}
					catch (Exception e)
					{
						log.WarnException("Could not update last query time of " + destination.ConnectionStringOptions.Url, e);
					}
				}
			}
			catch (Exception e)
			{
				log.ErrorException("Failed to send last queried timestamp of indexes", e);
			}
			finally
			{
				Monitor.Exit(_lastQueriedTaskLock);
			}
		}

		public void ReplicateIndexesAndTransformersTask(object state)
		{
			if (docDb.Disposed)
				return;

			if (Monitor.TryEnter(_indexReplicationTaskLock) == false)
				return;
			try
			{
				foreach (var destination in GetReplicationDestinations(x => x.SkipIndexReplication == false))
				{
					if (docDb.Indexes.Definitions.Length > 0)
					{
						foreach (var definition in docDb.Indexes.Definitions)
						{
							try
							{
								string url = destination.ConnectionStringOptions.Url + "/indexes/" + Uri.EscapeUriString(definition.Name);
								var replicationRequest = nonBufferedHttpRavenRequestFactory.Create(url, "PUT", destination.ConnectionStringOptions);
								replicationRequest.Write(RavenJObject.FromObject(definition));
								replicationRequest.ExecuteRequest();
							}
							catch (Exception e)
							{
								log.WarnException("Could not replicate index " + definition.Name + " to " + destination.ConnectionStringOptions.Url, e);
							}
						}
					}

					if (docDb.Transformers.Definitions.Length > 0)
					{
						foreach (var definition in docDb.Transformers.Definitions)
						{
							try
							{
								var clonedTransformer = definition.Clone();
								clonedTransformer.TransfomerId = 0;

								string url = destination.ConnectionStringOptions.Url + "/transformers/" + Uri.EscapeUriString(definition.Name);
								var replicationRequest = nonBufferedHttpRavenRequestFactory.Create(url, "PUT", destination.ConnectionStringOptions);
								replicationRequest.Write(RavenJObject.FromObject(clonedTransformer));
								replicationRequest.ExecuteRequest();
							}
							catch (Exception e)
							{
								log.WarnException("Could not replicate transformer " + definition.Name + " to " + destination.ConnectionStringOptions.Url, e);

							}
						}
					}
				}
			}
			catch (Exception e)
			{
				log.ErrorException("Failed to replicate indexes and transformers", e);
			}
			finally
			{
				Monitor.Exit(_indexReplicationTaskLock);
			}
		}


		private class JsonDocumentsToReplicate
		{
			public Etag LastEtag { get; set; }
			public DateTime LastLastModified { get; set; }
			public RavenJArray Documents { get; set; }
			public int CountOfFilteredDocumentsWhichAreSystemDocuments { get; set; }
			public int CountOfFilteredDocumentsWhichOriginFromDestination { get; set; }
			public List<JsonDocument> LoadedDocs { get; set; }
		}

		private JsonDocumentsToReplicate GetJsonDocuments(SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource, ReplicationStrategy destination, PrefetchingBehavior prefetchingBehavior, ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope scope)
		{
			var timeout = TimeSpan.FromSeconds(docDb.Configuration.Replication.FetchingFromDiskTimeoutInSeconds);
			var duration = Stopwatch.StartNew();
			var result = new JsonDocumentsToReplicate();
			try
			{
				var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();
				var maxNumberOfItemsToReceiveInSingleBatch = destinationsReplicationInformationForSource.MaxNumberOfItemsToReceiveInSingleBatch;

				docDb.TransactionalStorage.Batch(actions =>
				{
					var lastEtag = destinationsReplicationInformationForSource.LastDocumentEtag;

					int docsSinceLastReplEtag = 0;
					List<JsonDocument> docsToReplicate;
					List<JsonDocument> filteredDocsToReplicate;
					result.LastEtag = lastEtag;

					while (true)
					{
						docDb.WorkContext.CancellationToken.ThrowIfCancellationRequested();

						docsToReplicate = GetDocsToReplicate(actions, prefetchingBehavior, result, maxNumberOfItemsToReceiveInSingleBatch);

						filteredDocsToReplicate =
							docsToReplicate
								.Where(document =>
								{
									var info = docDb.Documents.GetRecentTouchesFor(document.Key);
									if (info != null)
									{
										if (info.TouchedEtag.CompareTo(result.LastEtag) > 0)
										{
											log.Debug(
												"Will not replicate document '{0}' to '{1}' because the updates after etag {2} are related document touches",
												document.Key, destinationId, info.TouchedEtag);
											return false;
										}
									}

									string reason;
									return destination.FilterDocuments(destinationId, document.Key, document.Metadata, out reason) &&
										   prefetchingBehavior.FilterDocuments(document);
								})
								.ToList();

						docsSinceLastReplEtag += docsToReplicate.Count;
						result.CountOfFilteredDocumentsWhichAreSystemDocuments +=
							docsToReplicate.Count(doc => destination.IsSystemDocumentId(doc.Key));
						result.CountOfFilteredDocumentsWhichOriginFromDestination +=
							docsToReplicate.Count(doc => destination.OriginsFromDestination(destinationId, doc.Metadata));

						if (docsToReplicate.Count > 0)
						{
							var lastDoc = docsToReplicate.Last();
							Debug.Assert(lastDoc.Etag != null);
							result.LastEtag = lastDoc.Etag;
							if (lastDoc.LastModified.HasValue)
								result.LastLastModified = lastDoc.LastModified.Value;
						}

						if (docsToReplicate.Count == 0 || filteredDocsToReplicate.Count != 0)
						{
							break;
						}

						log.Debug("All the docs were filtered, trying another batch from etag [>{0}]", result.LastEtag);

						if (duration.Elapsed > timeout)
							break;
					}

					log.Debug(() =>
					{
						if (docsSinceLastReplEtag == 0)
							return string.Format("No documents to replicate to {0} - last replicated etag: {1}", destination,
								lastEtag);

						if (docsSinceLastReplEtag == filteredDocsToReplicate.Count)
							return string.Format("Replicating {0} docs [>{1}] to {2}.",
								docsSinceLastReplEtag,
								lastEtag,
								destination);

						var diff = docsToReplicate.Except(filteredDocsToReplicate).Select(x => x.Key);
						return string.Format("Replicating {1} docs (out of {0}) [>{4}] to {2}. [Not replicated: {3}]",
							docsSinceLastReplEtag,
							filteredDocsToReplicate.Count,
							destination,
							string.Join(", ", diff),
							lastEtag);
					});

					scope.Record(new RavenJObject
		            {
		                {"StartEtag", lastEtag.ToString()},
		                {"EndEtag", result.LastEtag.ToString()},
		                {"Count", docsSinceLastReplEtag},
		                {"FilteredCount", filteredDocsToReplicate.Count}
		            });

					result.LoadedDocs = filteredDocsToReplicate;
					docDb.WorkContext.MetricsCounters.GetReplicationBatchSizeMetric(destination).Mark(docsSinceLastReplEtag);
					docDb.WorkContext.MetricsCounters.GetReplicationBatchSizeHistogram(destination).Update(docsSinceLastReplEtag);

					result.Documents = new RavenJArray(filteredDocsToReplicate
						.Select(x =>
						{
							JsonDocument.EnsureIdInMetadata(x);
							EnsureReplicationInformationInMetadata(x.Metadata, docDb);
							return x;
						})
						.Select(x => x.ToJson()));
				});
			}
			catch (Exception e)
			{
				scope.RecordError(e);
				log.WarnException(
					"Could not get documents to replicate after: " +
					destinationsReplicationInformationForSource.LastDocumentEtag, e);
			}
			return result;
		}

		private List<JsonDocument> GetDocsToReplicate(IStorageActionsAccessor actions, PrefetchingBehavior prefetchingBehavior, JsonDocumentsToReplicate result, int? maxNumberOfItemsToReceiveInSingleBatch)
		{
			var docsToReplicate = prefetchingBehavior.GetDocumentsBatchFrom(result.LastEtag, maxNumberOfItemsToReceiveInSingleBatch);
			Etag lastEtag = null;
			if (docsToReplicate.Count > 0)
				lastEtag = docsToReplicate[docsToReplicate.Count - 1].Etag;

			var maxNumberOfTombstones = Math.Max(1024, docsToReplicate.Count);
			var tombstones = actions
				.Lists
				.Read(Constants.RavenReplicationDocsTombstones, result.LastEtag, lastEtag, maxNumberOfTombstones + 1)
							.Select(x => new JsonDocument
							{
								Etag = x.Etag,
								Key = x.Key,
								Metadata = x.Data,
								DataAsJson = new RavenJObject()
							})
				.ToList();

			var results = docsToReplicate.Concat(tombstones);

			if (tombstones.Count >= maxNumberOfTombstones + 1)
			{
				var lastTombstoneEtag = tombstones[tombstones.Count - 1].Etag;
				log.Info("Replication batch trimmed. Found more than '{0}' document tombstones. Last etag from prefetcher: '{1}'. Last tombstone etag: '{2}'.", maxNumberOfTombstones, lastEtag, lastTombstoneEtag);

				results = results.Where(x => EtagUtil.IsGreaterThan(x.Etag, lastTombstoneEtag) == false);
			}

			results = results.OrderBy(x => x.Etag);

			// can't return earlier, because we need to know if there are tombstones that need to be send
			if (maxNumberOfItemsToReceiveInSingleBatch.HasValue) 
				results = results.Take(maxNumberOfItemsToReceiveInSingleBatch.Value);

			return results.ToList();
		}

		[Obsolete("Use RavenFS instead.")]
		private Tuple<RavenJArray, Etag> GetAttachments(SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource, ReplicationStrategy destination, ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope scope)
		{
			var timeout = TimeSpan.FromSeconds(docDb.Configuration.Replication.FetchingFromDiskTimeoutInSeconds);
			var duration = Stopwatch.StartNew();

			RavenJArray attachments = null;
			Etag lastAttachmentEtag = Etag.Empty;
			try
			{
				var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();
				var maxNumberOfItemsToReceiveInSingleBatch = destinationsReplicationInformationForSource.MaxNumberOfItemsToReceiveInSingleBatch;

				docDb.TransactionalStorage.Batch(actions =>
				{
					int attachmentSinceLastEtag = 0;
					List<AttachmentInformation> attachmentsToReplicate;
					List<AttachmentInformation> filteredAttachmentsToReplicate;
					var startEtag = destinationsReplicationInformationForSource.LastAttachmentEtag;
					lastAttachmentEtag = startEtag;
					while (true)
					{
						attachmentsToReplicate = GetAttachmentsToReplicate(actions, lastAttachmentEtag, maxNumberOfItemsToReceiveInSingleBatch);

						filteredAttachmentsToReplicate = attachmentsToReplicate.Where(attachment => destination.FilterAttachments(attachment, destinationId)).ToList();

						attachmentSinceLastEtag += attachmentsToReplicate.Count;

						if (attachmentsToReplicate.Count == 0 ||
							filteredAttachmentsToReplicate.Count != 0)
						{
							break;
						}

						AttachmentInformation jsonDocument = attachmentsToReplicate.Last();
						Etag attachmentEtag = jsonDocument.Etag;
						log.Debug("All the attachments were filtered, trying another batch from etag [>{0}]", attachmentEtag);
						lastAttachmentEtag = attachmentEtag;

						if (duration.Elapsed > timeout)
							break;
					}

					log.Debug(() =>
					{
						if (attachmentSinceLastEtag == 0)
							return string.Format("No attachments to replicate to {0} - last replicated etag: {1}", destination,
												 destinationsReplicationInformationForSource.LastAttachmentEtag);

						if (attachmentSinceLastEtag == filteredAttachmentsToReplicate.Count)
							return string.Format("Replicating {0} attachments [>{1}] to {2}.",
											 attachmentSinceLastEtag,
											 destinationsReplicationInformationForSource.LastAttachmentEtag,
											 destination);

						var diff = attachmentsToReplicate.Except(filteredAttachmentsToReplicate).Select(x => x.Key);
						return string.Format("Replicating {1} attachments (out of {0}) [>{4}] to {2}. [Not replicated: {3}]",
											 attachmentSinceLastEtag,
											 filteredAttachmentsToReplicate.Count,
											 destination,
											 string.Join(", ", diff),
											 destinationsReplicationInformationForSource.LastAttachmentEtag);
					});

					scope.Record(new RavenJObject
					             {
						             {"StartEtag", startEtag.ToString()},
									 {"EndEtag", lastAttachmentEtag.ToString()},
									 {"Count", attachmentSinceLastEtag},
									 {"FilteredCount", filteredAttachmentsToReplicate.Count}
					             });

					attachments = new RavenJArray(filteredAttachmentsToReplicate
													  .Select(x =>
													  {
														  var data = new byte[0];
														  if (x.Size > 0)
														  {
															  data = actions.Attachments.GetAttachment(x.Key).Data().ReadData();
														  }

														  EnsureReplicationInformationInMetadata(x.Metadata, docDb);

														  return new RavenJObject
							                                           {
								                                           {"@metadata", x.Metadata},
								                                           {"@id", x.Key},
								                                           {"@etag", x.Etag.ToByteArray()},
								                                           {"data", data}
							                                           };
													  }));
				});
			}
			catch (InvalidDataException e)
			{
				RecordFailure(String.Empty, string.Format("Data is corrupted, could not proceed with attachment replication. Exception : {0}", e));
				scope.RecordError(e);
				log.ErrorException("Data is corrupted, could not proceed with replication", e);
			}
			catch (Exception e)
			{
				log.WarnException("Could not get attachments to replicate after: " + destinationsReplicationInformationForSource.LastAttachmentEtag, e);
			}
			return Tuple.Create(attachments, lastAttachmentEtag);
		}

		[Obsolete("Use RavenFS instead.")]
		private static List<AttachmentInformation> GetAttachmentsToReplicate(IStorageActionsAccessor actions, Etag lastAttachmentEtag, int? maxNumberOfItemsToReceiveInSingleBatch)
		{
			var maxNumberOfAttachments = 100;
			if (maxNumberOfItemsToReceiveInSingleBatch.HasValue) 
				maxNumberOfAttachments = Math.Min(maxNumberOfAttachments, maxNumberOfItemsToReceiveInSingleBatch.Value);

			var attachmentInformations = actions.Attachments.GetAttachmentsAfter(lastAttachmentEtag, maxNumberOfAttachments, 1024 * 1024 * 10).ToList();

			Etag lastEtag = null;
			if (attachmentInformations.Count > 0)
				lastEtag = attachmentInformations[attachmentInformations.Count - 1].Etag;

			var maxNumberOfTombstones = Math.Max(maxNumberOfAttachments, attachmentInformations.Count);
			var tombstones = actions
				.Lists
				.Read(Constants.RavenReplicationAttachmentsTombstones, lastAttachmentEtag, lastEtag, maxNumberOfTombstones + 1)
							.Select(x => new AttachmentInformation
							{
								Key = x.Key,
								Etag = x.Etag,
								Metadata = x.Data,
								Size = 0,
							})
				.ToList();

			var results = attachmentInformations.Concat(tombstones);

			if (tombstones.Count >= maxNumberOfTombstones + 1)
			{
				var lastTombstoneEtag = tombstones[tombstones.Count - 1].Etag;
				log.Info("Replication batch trimmed. Found more than '{0}' attachment tombstones. Last attachment etag: '{1}'. Last tombstone etag: '{2}'.", maxNumberOfTombstones, lastEtag, lastTombstoneEtag);

				results = results.Where(x => EtagUtil.IsGreaterThan(x.Etag, lastTombstoneEtag) == false);
			}

			results = results.OrderBy(x => x.Etag);

			// can't return earlier, because we need to know if there are tombstones that need to be send
			if (maxNumberOfItemsToReceiveInSingleBatch.HasValue)
				results = results.Take(maxNumberOfItemsToReceiveInSingleBatch.Value);

			return results.ToList();
		}

		internal SourceReplicationInformationWithBatchInformation GetLastReplicatedEtagFrom(ReplicationStrategy destination)
		{
			try
			{
				Etag currentEtag = Etag.Empty;
				docDb.TransactionalStorage.Batch(accessor => currentEtag = accessor.Staleness.GetMostRecentDocumentEtag());
				var url = destination.ConnectionStringOptions.Url + "/replication/lastEtag?from=" + UrlEncodedServerUrl() +
						  "&currentEtag=" + currentEtag + "&dbid=" + docDb.TransactionalStorage.Id;
				var request = httpRavenRequestFactory.Create(url, "GET", destination.ConnectionStringOptions);
				var lastReplicatedEtagFrom = request.ExecuteRequest<SourceReplicationInformationWithBatchInformation>();
				return lastReplicatedEtagFrom;
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound))
					log.WarnException("Replication is not enabled on: " + destination, e);
				else
					log.WarnException("Failed to contact replication destination: " + destination, e);
				RecordFailure(destination.ConnectionStringOptions.Url, e.Message);
			}
			catch (Exception e)
			{
				log.WarnException("Failed to contact replication destination: " + destination, e);
				RecordFailure(destination.ConnectionStringOptions.Url, e.Message);
			}

			return null;
		}

		private string UrlEncodedServerUrl()
		{
			return Uri.EscapeDataString(docDb.ServerUrl);
		}

		internal ReplicationStrategy[] GetReplicationDestinations(Predicate<ReplicationDestination> predicate = null)
		{
			ConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>> configurationDocument;
			try
			{
				configurationDocument = docDb.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
			}
			catch (Exception e)
			{
				log.Warn("Cannot get replication destinations", e);
				return new ReplicationStrategy[0];
			}

			if (configurationDocument == null)
			{
				return new ReplicationStrategy[0];
			}

			var replicationDocument = configurationDocument.MergedDocument;

			if (configurationDocument.LocalExists && string.IsNullOrWhiteSpace(replicationDocument.Source))
			{
				replicationDocument.Source = docDb.TransactionalStorage.Id.ToString();
				try
				{
					var ravenJObject = RavenJObject.FromObject(replicationDocument);
					ravenJObject.Remove("Id");
					docDb.Documents.Put(Constants.RavenReplicationDestinations, configurationDocument.Etag, ravenJObject, configurationDocument.Metadata, null);
				}
				catch (ConcurrencyException)
				{
					// we will get it next time
				}
			}

			if (replicationDocument.Source != docDb.TransactionalStorage.Id.ToString())
			{
				if (!wrongReplicationSourceAlertSent)
				{
					var dbName = string.IsNullOrEmpty(docDb.Name) ? "<system>" : docDb.Name;

					docDb.AddAlert(new Alert
						{
							AlertLevel = AlertLevel.Error,
							CreatedAt = SystemTime.UtcNow,
							Message = "Source of the ReplicationDestinations document is not the same as the database it is located in",
							Title = "Wrong replication source: " + replicationDocument.Source + " instead of " + docDb.TransactionalStorage.Id + " in database " + dbName,
							UniqueKey = "Wrong source: " + replicationDocument.Source + ", " + docDb.TransactionalStorage.Id
						});

					wrongReplicationSourceAlertSent = true;
				}

				return new ReplicationStrategy[0];
			}

			wrongReplicationSourceAlertSent = false;

			return replicationDocument
				.Destinations
				.Where(x => !x.Disabled)
				.Where(x => predicate == null || predicate(x))
				.Select(GetConnectionOptionsSafe)
				.Where(x => x != null)
				.ToArray();
		}

		private ReplicationStrategy GetConnectionOptionsSafe(ReplicationDestination x)
		{
			try
			{
				return GetConnectionOptions(x, docDb);
			}
			catch (Exception e)
			{
				log.ErrorException(
					string.Format("IGNORING BAD REPLICATION CONFIG!{0}Could not figure out connection options for [Url: {1}, ClientVisibleUrl: {2}]",
					Environment.NewLine, x.Url, x.ClientVisibleUrl),
					e);

				return null;
			}
		}

		public static ReplicationStrategy GetConnectionOptions(ReplicationDestination x, DocumentDatabase database)
		{
			var replicationStrategy = new ReplicationStrategy
			{
				ReplicationOptionsBehavior = x.TransitiveReplicationBehavior,
				CurrentDatabaseId = database.TransactionalStorage.Id.ToString()
			};
			return CreateReplicationStrategyFromDocument(x, replicationStrategy);
		}

		private static ReplicationStrategy CreateReplicationStrategyFromDocument(ReplicationDestination x, ReplicationStrategy replicationStrategy)
		{
			var url = x.Url;
			if (string.IsNullOrEmpty(x.Database) == false)
			{
				url = url + "/databases/" + x.Database;
			}
			replicationStrategy.ConnectionStringOptions = new RavenConnectionStringOptions
			{
				Url = url,
				ApiKey = x.ApiKey,
			};
			if (string.IsNullOrEmpty(x.Username) == false)
			{
				replicationStrategy.ConnectionStringOptions.Credentials = string.IsNullOrEmpty(x.Domain)
					? new NetworkCredential(x.Username, x.Password)
					: new NetworkCredential(x.Username, x.Password, x.Domain);
			}
			return replicationStrategy;
		}

		public void HandleHeartbeat(string src)
		{
			ResetFailureForHeartbeat(src);

			heartbeatDictionary.AddOrUpdate(src, SystemTime.UtcNow, (_, __) => SystemTime.UtcNow);
		}

		public bool IsHeartbeatAvailable(string src, DateTime lastCheck)
		{
			if (heartbeatDictionary.ContainsKey(src))
			{
				DateTime lastHeartbeat;
				if (heartbeatDictionary.TryGetValue(src, out lastHeartbeat))
				{
					return lastHeartbeat >= lastCheck;
				}
			}

			return false;
		}


		private void ResetFailureForHeartbeat(string src)
		{
			RecordSuccess(src, lastHeartbeatReceived: SystemTime.UtcNow);
			docDb.WorkContext.ShouldNotifyAboutWork(() => "Replication Heartbeat from " + src);
			docDb.WorkContext.NotifyAboutWork();
		}

		public void Dispose()
		{
			_indexReplicationTaskTimer.Dispose();
			_lastQueriedTaskTimer.Dispose();

			Task task;
			while (activeTasks.TryDequeue(out task))
			{
				task.Wait();
			}

			foreach (var prefetchingBehavior in prefetchingBehaviors)
			{
				prefetchingBehavior.Value.Dispose();
			}
		}

		private readonly ConcurrentDictionary<string, DateTime> heartbeatDictionary = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

		internal static void EnsureReplicationInformationInMetadata(RavenJObject metadata, DocumentDatabase database)
		{
			Debug.Assert(database != null);

			if (metadata == null)
				return;

			if (metadata.ContainsKey(Constants.RavenReplicationSource))
				return;

			metadata[Constants.RavenReplicationHistory] = new RavenJArray();
			metadata[Constants.RavenReplicationVersion] = 0;
			metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(database.TransactionalStorage.Id);
		}
	}

	internal class ReplicationStatisticsRecorder : IDisposable
	{
		private readonly ReplicationStrategy destination;

		private readonly ConcurrentDictionary<string, DestinationStats> destinationStats;

		private readonly RavenJObject record;

		private readonly RavenJArray records;

		private readonly Stopwatch watch;

		public ReplicationStatisticsRecorder(ReplicationStrategy destination, ConcurrentDictionary<string, DestinationStats> destinationStats)
		{
			this.destination = destination;
			this.destinationStats = destinationStats;
			watch = Stopwatch.StartNew();
			Started = SystemTime.UtcNow;
			records = new RavenJArray();
			record = new RavenJObject
			         {
				         { "Url", destination.ConnectionStringOptions.Url },
						 { "StartTime", SystemTime.UtcNow},
						 { "Records", records }
			         };
		}

		public DateTime Started { get; private set; }


		public TimeSpan ElapsedTime
		{
			get
			{
				return watch.Elapsed;
			}
		}

		public void Dispose()
		{
			record.Add("TotalExecutionTime", watch.Elapsed.ToString());

			var stats = destinationStats.GetOrDefault(destination.ConnectionStringOptions.Url, new DestinationStats { Url = destination.ConnectionStringOptions.Url });

			stats.LastStats.Insert(0, record);

			while (stats.LastStats.Length > 50)
				stats.LastStats.RemoveAt(stats.LastStats.Length - 1);
		}

		public ReplicationStatisticsRecorderScope StartRecording(string name)
		{
			var scopeRecord = new RavenJObject();
			records.Add(scopeRecord);
			return new ReplicationStatisticsRecorderScope(name, scopeRecord);
		}

		internal class ReplicationStatisticsRecorderScope : IDisposable
		{
			private readonly RavenJObject record;

			private readonly RavenJArray records;

			private readonly Stopwatch watch;

			public ReplicationStatisticsRecorderScope(string name, RavenJObject record)
			{
				this.record = record;
				records = new RavenJArray();

				record.Add("Name", name);
				record.Add("Records", records);

				watch = Stopwatch.StartNew();
			}

			public void Dispose()
			{
				record.Add("ExecutionTime", watch.Elapsed.ToString());
			}

			public void Record(RavenJObject value)
			{
				records.Add(value);
			}

			public void RecordError(Exception exception)
			{
				records.Add(new RavenJObject
				            {
					            { "Error", new RavenJObject
					                       {
						                       { "Type", exception.GetType().Name }, 
											   { "Message", exception.Message }
					                       } }
				            });
			}

			public void RecordError(string error)
			{
				records.Add(new RavenJObject
				            {
					            { "Error", error }
				            });
			}

			public ReplicationStatisticsRecorderScope StartRecording(string name)
			{
				var scopeRecord = new RavenJObject();
				records.Add(scopeRecord);
				return new ReplicationStatisticsRecorderScope(name, scopeRecord);
			}
		}
	}
}
