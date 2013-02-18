//-----------------------------------------------------------------------
// <copyright file="IndexStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Queries;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Directory = System.IO.Directory;
using System.ComponentModel.Composition;

namespace Raven.Database.Indexing
{
	/// <summary>
	/// 	Thread safe, single instance for the entire application
	/// </summary>
	public class IndexStorage : CriticalFinalizerObject, IDisposable
	{
		private readonly DocumentDatabase documentDatabase;
		private const string IndexVersion = "2.0.0.1";

		private readonly IndexDefinitionStorage indexDefinitionStorage;
		private readonly InMemoryRavenConfiguration configuration;
		private readonly string path;
		private readonly ConcurrentDictionary<string, Index> indexes = new ConcurrentDictionary<string, Index>(StringComparer.InvariantCultureIgnoreCase);
		private static readonly ILog log = LogManager.GetCurrentClassLogger();
		private static readonly ILog startupLog = LogManager.GetLogger(typeof(IndexStorage).FullName + ".Startup");
		private readonly Analyzer dummyAnalyzer = new SimpleAnalyzer();
		private DateTime latestPersistedQueryTime;
		private readonly FileStream crashMarker;

		public IndexStorage(IndexDefinitionStorage indexDefinitionStorage, InMemoryRavenConfiguration configuration, DocumentDatabase documentDatabase)
		{
			try
			{
				this.indexDefinitionStorage = indexDefinitionStorage;
				this.configuration = configuration;
				this.documentDatabase = documentDatabase;
				path = configuration.IndexStoragePath;

				if (Directory.Exists(path) == false && configuration.RunInMemory == false)
					Directory.CreateDirectory(path);


				if (configuration.RunInMemory == false)
				{
					var crashMarkerPath = Path.Combine(path, "indexing.crash-marker");

					if (File.Exists(crashMarkerPath))
					{
						// the only way this can happen is if we crashed because of a power outage
						// in this case, we consider all open indexes to be corrupt and force them
						// to be reset. This is because to get better perf, we don't flush the files to disk,
						// so in the case of a power outage, we can't be sure that there wasn't still stuff in
						// the OS buffer that wasn't written yet.
						configuration.ResetIndexOnUncleanShutdown = true;
					}

					// The delete on close ensures that the only way this file will exists is if there was
					// a power outage while the server was running.
					crashMarker = File.Create(crashMarkerPath, 16, FileOptions.DeleteOnClose);
				}

				BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(documentDatabase.WorkContext, 
					indexDefinitionStorage.IndexNames, OpenIndexOnStartup);
			}
			catch
			{
				Dispose();
				throw;
			}
		}

		private void OpenIndexOnStartup(string indexName)
		{
			if (indexName == null) throw new ArgumentNullException("indexName");

			startupLog.Debug("Loading saved index {0}", indexName);

			var indexDefinition = indexDefinitionStorage.GetIndexDefinition(indexName);
			if (indexDefinition == null)
				return;

			Index indexImplementation;
			bool resetTried = false;
			bool recoveryTried = false;
			string[] keysToDeleteAfterRecovery = null;
			while (true)
			{
				Lucene.Net.Store.Directory luceneDirectory = null;
				
				try
				{
					luceneDirectory = OpenOrCreateLuceneDirectory(indexDefinition, createIfMissing: resetTried);
					indexImplementation = CreateIndexImplementation(indexName, indexDefinition, luceneDirectory);

					if (indexImplementation is SimpleIndex && keysToDeleteAfterRecovery != null)
					{
						// remove keys from index that were deleted after creating commit point
						((SimpleIndex) indexImplementation).RemoveDirectlyFromIndex(keysToDeleteAfterRecovery);
					}

					LoadExistingSuggestionsExtentions(indexName, indexImplementation);
					documentDatabase.TransactionalStorage.Batch(accessor =>
					{
						var read = accessor.Lists.Read("Raven/Indexes/QueryTime", indexName);
						if (read == null)
							return;

						var dateTime = read.Data.Value<DateTime>("LastQueryTime");
						indexImplementation.MarkQueried(dateTime);
						if (dateTime > latestPersistedQueryTime)
							latestPersistedQueryTime = dateTime;
					});
					break;
				}
				catch (Exception e)
				{
					if (resetTried)
						throw new InvalidOperationException("Could not open / create index" + indexName + ", reset already tried", e);

					if (recoveryTried == false)
					{
						recoveryTried = true;

						startupLog.WarnException("Could not open index " + indexName + ". Trying to recover index", e);

						if (indexDefinition.IsMapReduce == false)
						{
							IndexCommitPoint commitUsedToRestore;

							if (TryReusePreviousCommitPointsToRecoverIndex(luceneDirectory,
							                                               indexDefinition, path,
							                                               out commitUsedToRestore,
							                                               out keysToDeleteAfterRecovery))
							{
								ResetLastIndexedEtagAccordingToRestoredCommitPoint(indexDefinition, commitUsedToRestore);
							}
						}
						else
						{
							RegenerateMapReduceIndex(luceneDirectory, indexDefinition.Name);
						}
					}
					else
					{
						resetTried = true;
						startupLog.WarnException("Could not open index " + indexName + ". Recovery operation failed, orcibly resetting index", e);
						try
						{
							documentDatabase.TransactionalStorage.Batch(accessor =>
							{
								accessor.Indexing.DeleteIndex(indexName);
								accessor.Indexing.AddIndex(indexName, indexDefinition.IsMapReduce);
							});

							var indexDirectory = indexName;
							var indexFullPath = Path.Combine(path, MonoHttpUtility.UrlEncode(indexDirectory));
							IOExtensions.DeleteDirectory(indexFullPath);
						}
						catch (Exception exception)
						{
							throw new InvalidOperationException("Could not reset index " + indexName, exception);
						}
					}
				}
			}
			indexes.TryAdd(indexName, indexImplementation);
		}

		private void LoadExistingSuggestionsExtentions(string indexName, Index indexImplementation)
		{
			var suggestionsForIndex = Path.Combine(configuration.IndexStoragePath, "Raven-Suggestions", indexName);
			if (!Directory.Exists(suggestionsForIndex))
				return;

			foreach (var directory in Directory.GetDirectories(suggestionsForIndex))
			{
				IndexSearcher searcher;
				using (indexImplementation.GetSearcher(out searcher))
				{
					var key = Path.GetFileName(directory);
					var decodedKey = MonoHttpUtility.UrlDecode(key);
					var lastIndexOfDash = decodedKey.LastIndexOf('-');
					var accuracy = float.Parse(decodedKey.Substring(lastIndexOfDash + 1));
					var lastIndexOfDistance = decodedKey.LastIndexOf('-', lastIndexOfDash - 1);
					StringDistanceTypes distanceType;
					Enum.TryParse(decodedKey.Substring(lastIndexOfDistance + 1, lastIndexOfDash - lastIndexOfDistance - 1),
								  true, out distanceType);
					var field = decodedKey.Substring(0, lastIndexOfDistance);
					var extension = new SuggestionQueryIndexExtension(
						documentDatabase.WorkContext, 
						Path.Combine(configuration.IndexStoragePath, "Raven-Suggestions", indexName, key), searcher.IndexReader.Directory() is RAMDirectory,
						SuggestionQueryRunner.GetStringDistance(distanceType),
						field,
						accuracy);
					indexImplementation.SetExtension(key, extension);
				}
			}
		}


		protected Lucene.Net.Store.Directory OpenOrCreateLuceneDirectory(
			IndexDefinition indexDefinition,
			string indexName = null,
			bool createIfMissing = true)
		{
			Lucene.Net.Store.Directory directory;
			if (indexDefinition.IsTemp || configuration.RunInMemory)
			{
				directory = new RAMDirectory();
				new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose(); // creating index structure
			}
			else
			{
				var indexDirectory = indexName ?? IndexDefinitionStorage.FixupIndexName(indexDefinition.Name, path);
				var indexFullPath = Path.Combine(path, MonoHttpUtility.UrlEncode(indexDirectory));
				directory = new LuceneCodecDirectory(indexFullPath, documentDatabase.IndexCodecs.OfType<AbstractIndexCodec>());

				if (!IndexReader.IndexExists(directory))
				{
					if (createIfMissing == false)
						throw new InvalidOperationException("Index does not exists: " + indexDirectory);

					WriteIndexVersion(directory);

					//creating index structure if we need to
					new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();
				}
				else
				{
					EnsureIndexVersionMatches(indexName, directory);
					if (directory.FileExists("write.lock"))// force lock release, because it was still open when we shut down
					{
						IndexWriter.Unlock(directory);
						// for some reason, just calling unlock doesn't remove this file
						directory.DeleteFile("write.lock");
					} 
					if (directory.FileExists("writing-to-index.lock")) // we had an unclean shutdown
					{
						if (configuration.ResetIndexOnUncleanShutdown)
							throw new InvalidOperationException("Rude shutdown detected on: " + indexDirectory);

						CheckIndexAndTryToFix(directory, indexDirectory);
						directory.DeleteFile("writing-to-index.lock");
					}
				}
			}

			return directory;

		}

		private void RegenerateMapReduceIndex(Lucene.Net.Store.Directory directory, string indexName)
		{
			// remove old index data
			var dirOnDisk = Path.Combine(path, MonoHttpUtility.UrlEncode(indexName));
			IOExtensions.DeleteDirectory(dirOnDisk);

			// initialize by new index
			Directory.CreateDirectory(dirOnDisk);
			WriteIndexVersion(directory);
			new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();

			var start = 0;
			const int take = 100;

			documentDatabase.TransactionalStorage.Batch(actions =>
			{
				IList<ReduceTypePerKey> reduceKeysAndTypes;

				do
				{
					reduceKeysAndTypes = actions.MapReduce.GetReduceKeysAndTypes(indexName, start, take).ToList();
					start += take;

					var keysToScheduleOnLevel2 =
						reduceKeysAndTypes.Where(x => x.OperationTypeToPerform == ReduceType.MultiStep).ToList();
					var keysToScheduleOnLevel0 =
						reduceKeysAndTypes.Where(x => x.OperationTypeToPerform == ReduceType.SingleStep).ToList();

					var itemsToScheduleOnLevel2 = keysToScheduleOnLevel2.Select(x => new ReduceKeyAndBucket(0, x.ReduceKey)).ToList();
					var itemsToScheduleOnLevel0 = new List<ReduceKeyAndBucket>();

					foreach (var reduceKey in keysToScheduleOnLevel0.Select(x => x.ReduceKey))
					{
						var mappedBuckets = actions.MapReduce.GetMappedBuckets(indexName, reduceKey).Distinct();

						itemsToScheduleOnLevel0.AddRange(mappedBuckets.Select(x => new ReduceKeyAndBucket(x, reduceKey)));

						actions.General.MaybePulseTransaction();
					}

					foreach (var itemToReduce in itemsToScheduleOnLevel2)
					{
						actions.MapReduce.ScheduleReductions(indexName, 2, itemToReduce);
					}

					foreach (var itemToReduce in itemsToScheduleOnLevel0)
					{
						actions.MapReduce.ScheduleReductions(indexName, 0, itemToReduce);
					}
					
					actions.General.MaybePulseTransaction();
				} while (reduceKeysAndTypes.Count > 0);
			});
		}

		private void ResetLastIndexedEtagAccordingToRestoredCommitPoint(IndexDefinition indexDefinition,
		                                                                IndexCommitPoint lastCommitPoint)
		{
			documentDatabase.TransactionalStorage.Batch(
				accessor =>
				accessor.Indexing.UpdateLastIndexed(indexDefinition.Name, lastCommitPoint.HighestCommitedETag,
				                                    lastCommitPoint.TimeStamp));
		}

		private static void WriteIndexVersion(Lucene.Net.Store.Directory directory)
		{
			using(var indexOutput = directory.CreateOutput("index.version"))
			{
				indexOutput.WriteString(IndexVersion);
				indexOutput.Flush();
			}
		}

		private static void EnsureIndexVersionMatches(string indexName, Lucene.Net.Store.Directory directory)
		{
			if (directory.FileExists("index.version") == false)
			{
				throw new InvalidOperationException("Could not find index.version " + indexName + ", resetting index");
			}
			using(var indexInput = directory.OpenInput("index.version"))
			{
				var versionFromDisk = indexInput.ReadString();
				if (versionFromDisk != IndexVersion)
					throw new InvalidOperationException("Index " + indexName + " is of version " + versionFromDisk +
														" which is not compatible with " + IndexVersion + ", resetting index");
			}
		}

		private static void CheckIndexAndTryToFix(Lucene.Net.Store.Directory directory, string indexDirectory)
		{
			startupLog.Warn("Unclean shutdown detected on {0}, checking the index for errors. This may take a while.", indexDirectory);

			var memoryStream = new MemoryStream();
			var stringWriter = new StreamWriter(memoryStream);
			var checkIndex = new CheckIndex(directory);

			if (startupLog.IsWarnEnabled)
				checkIndex.SetInfoStream(stringWriter);

			var sp = Stopwatch.StartNew();
			var status = checkIndex.CheckIndex_Renamed_Method();
			sp.Stop();
			if (startupLog.IsWarnEnabled)
			{
				startupLog.Warn("Checking index {0} took: {1}, clean: {2}", indexDirectory, sp.Elapsed, status.clean);
				memoryStream.Position = 0;

				log.Warn(new StreamReader(memoryStream).ReadToEnd());
			}

			if (status.clean)
				return;

			startupLog.Warn("Attempting to fix index: {0}", indexDirectory);
			sp.Restart();
			checkIndex.FixIndex(status);
			startupLog.Warn("Fixed index {0} in {1}", indexDirectory, sp.Elapsed);
		}

		public void StoreCommitPoint(string indexName, IndexCommitPoint indexCommit)
		{
			if (indexCommit.SegmentsInfo == null || indexCommit.SegmentsInfo.IsIndexCorrupted) 
				return;

			var directoryName = indexCommit.SegmentsInfo.Generation.ToString("0000000000000000000", CultureInfo.InvariantCulture);
			var commitPointDirectory = new IndexCommitPointDirectory(path, indexName, directoryName);

			if (Directory.Exists(commitPointDirectory.AllCommitPointsFullPath) == false)
			{
				Directory.CreateDirectory(commitPointDirectory.AllCommitPointsFullPath);
			}	

			Directory.CreateDirectory(commitPointDirectory.FullPath);

			using (var commitPointFile = File.Create(commitPointDirectory.FileFullPath))
			{
				using (var sw = new StreamWriter(commitPointFile))
				{
					var jsonSerializer = new JsonSerializer();
					var textWriter = new JsonTextWriter(sw);

					jsonSerializer.Serialize(textWriter, indexCommit);
						
					sw.Flush();
				}
			}

			var currentSegmentsFileName = indexCommit.SegmentsInfo.SegmentsFileName;

			File.Copy(Path.Combine(commitPointDirectory.IndexFullPath, currentSegmentsFileName),
			          Path.Combine(commitPointDirectory.FullPath, currentSegmentsFileName));

			var storedCommitPoints = Directory.GetDirectories(commitPointDirectory.AllCommitPointsFullPath);

			if (storedCommitPoints.Length > configuration.MaxNumberOfStoredCommitPoints)
			{
				foreach (var toDelete in storedCommitPoints.Take(storedCommitPoints.Length - configuration.MaxNumberOfStoredCommitPoints))
				{
					IOExtensions.DeleteDirectory(toDelete);
				}
			}
		}

		public void AddDeletedKeysToCommitPoints(string indexName, string[] deletedKeys)
		{
			var sb = new StringBuilder();

			foreach (var deletedKey in deletedKeys)
			{
				sb.AppendLine(deletedKey);
			}

			var keysToDeleteAfterRecovery = sb.ToString();

			var indexFullPath = Path.Combine(path, MonoHttpUtility.UrlEncode(indexName));

			var existingCommitPoints =
				IndexCommitPointDirectory.ScanAllCommitPointsDirectory(indexFullPath);

			foreach (var commitPoint in existingCommitPoints)
			{
				var commitPointDirectory = new IndexCommitPointDirectory(path, indexName, commitPoint);

				File.AppendAllText(commitPointDirectory.DeletedKeysFile, keysToDeleteAfterRecovery);
			}
		}

		private static bool TryReusePreviousCommitPointsToRecoverIndex(Lucene.Net.Store.Directory directory, IndexDefinition indexDefinition, string indexStoragePath, out IndexCommitPoint indexCommit, out string[] keysToDelete)
		{
			indexCommit = null;
			keysToDelete = null;

			if (indexDefinition.IsMapReduce)
				return false;

			var indexFullPath = Path.Combine(indexStoragePath, MonoHttpUtility.UrlEncode(indexDefinition.Name));

			var allCommitPointsFullPath = IndexCommitPointDirectory.GetAllCommitPointsFullPath(indexFullPath);

			if (Directory.Exists(allCommitPointsFullPath) == false)
				return false;

			var filesInIndexDirectory = Directory.GetFiles(indexFullPath).Select(Path.GetFileName);

			var existingCommitPoints =
				IndexCommitPointDirectory.ScanAllCommitPointsDirectory(indexFullPath);

			Array.Reverse(existingCommitPoints); // start from the highest generation

			foreach (var commitPointDirectoryName in existingCommitPoints)
			{
				try
				{
					var commitPointDirectory = new IndexCommitPointDirectory(indexStoragePath, indexDefinition.Name,
																				commitPointDirectoryName);

					using (var commitPointFile = File.Open(commitPointDirectory.FileFullPath, FileMode.Open))
					{
						var jsonSerializer = new JsonSerializer();
						var textReader = new JsonTextReader(new StreamReader(commitPointFile));

						indexCommit = jsonSerializer.Deserialize<IndexCommitPoint>(textReader);
					}

					var missingFile =
						indexCommit.SegmentsInfo.ReferencedFiles.Any(
							referencedFile => filesInIndexDirectory.Contains(referencedFile) == false);

					if (missingFile)
					{
						IOExtensions.DeleteDirectory(commitPointDirectory.FullPath);
						continue; // there are some missing files, try another commit point
					}

					var storedSegmentsFile = indexCommit.SegmentsInfo.SegmentsFileName;

					// here there should be only one segments_N file, however remove all if there is more
					foreach (var currentSegmentsFile in Directory.GetFiles(commitPointDirectory.IndexFullPath, "segments_*"))
					{
						File.Delete(currentSegmentsFile);
					}

					// copy old segments_N file
					File.Copy(Path.Combine(commitPointDirectory.FullPath, storedSegmentsFile),
					          Path.Combine(commitPointDirectory.IndexFullPath, storedSegmentsFile), true);

					try
					{
						// update segments.gen file
						using (var genOutput = directory.CreateOutput(IndexFileNames.SEGMENTS_GEN))
						{
							genOutput.WriteInt(SegmentInfos.FORMAT_LOCKLESS);
							genOutput.WriteLong(indexCommit.SegmentsInfo.Generation);
							genOutput.WriteLong(indexCommit.SegmentsInfo.Generation);
						}
					}
					catch (Exception)
					{
						// here we can ignore, segments.gen is used only as fallback
					}

					if (File.Exists(commitPointDirectory.DeletedKeysFile))
						keysToDelete = File.ReadLines(commitPointDirectory.DeletedKeysFile).ToArray();

					return true;
				}
				catch (Exception ex)
				{
					log.DebugException("Could not recover an index named '" + indexDefinition.Name +
					                   "'from segments of the following generation " + commitPointDirectoryName, ex);
				}
			}

			return false;
		}

		internal Lucene.Net.Store.Directory MakeRAMDirectoryPhysical(RAMDirectory ramDir, string indexName)
		{
			var newDir = new LuceneCodecDirectory(Path.Combine(path, MonoHttpUtility.UrlEncode(IndexDefinitionStorage.FixupIndexName(indexName, path))), documentDatabase.IndexCodecs.OfType<AbstractIndexCodec>());
			Lucene.Net.Store.Directory.Copy(ramDir, newDir, true);
			return newDir;
		}

		private Index CreateIndexImplementation(string directoryPath, IndexDefinition indexDefinition, Lucene.Net.Store.Directory directory)
		{
			var viewGenerator = indexDefinitionStorage.GetViewGenerator(indexDefinition.Name);
			var indexImplementation = indexDefinition.IsMapReduce
										? (Index)new MapReduceIndex(directory, directoryPath, indexDefinition, viewGenerator, documentDatabase.WorkContext)
										: new SimpleIndex(directory, directoryPath, indexDefinition, viewGenerator, documentDatabase.WorkContext);

			configuration.Container.SatisfyImportsOnce(indexImplementation);

			return indexImplementation;
		}

		public string[] Indexes
		{
			get { return indexes.Keys.ToArray(); }
		}

		public bool HasIndex(string index)
		{
			if (index == null)
				return false;
			return indexes.ContainsKey(index);
		}

		public void Dispose()
		{
			var exceptionAggregator = new ExceptionAggregator(log, "Could not properly close index storage");

			exceptionAggregator.Execute(() => Parallel.ForEach(indexes.Values, index => exceptionAggregator.Execute(index.Dispose)));

			exceptionAggregator.Execute(() => dummyAnalyzer.Close());

			exceptionAggregator.Execute(() =>
			{
				if (crashMarker != null)
					crashMarker.Dispose();
			});

			exceptionAggregator.ThrowIfNeeded();
		}

		public void DeleteIndex(string name)
		{
			Index value;
			if (indexes.TryGetValue(name, out value) == false)
			{
				log.Debug("Ignoring delete for non existing index {0}", name);
				return;
			}
			log.Debug("Deleting index {0}", name);
			value.Dispose();
			Index ignored;
			var dirOnDisk = Path.Combine(path, MonoHttpUtility.UrlEncode(name));

			documentDatabase.TransactionalStorage.Batch(accessor =>
				accessor.Lists.Remove("Raven/Indexes/QueryTime", name));

			if (!indexes.TryRemove(name, out ignored) || !Directory.Exists(dirOnDisk))
				return;

			IOExtensions.DeleteDirectory(dirOnDisk);
		}

		public void CreateIndexImplementation(IndexDefinition indexDefinition)
		{
			var encodedName = IndexDefinitionStorage.FixupIndexName(indexDefinition.Name, path);
			log.Debug("Creating index {0} with encoded name {1}", indexDefinition.Name, encodedName);

			IndexDefinitionStorage.ResolveAnalyzers(indexDefinition);
			AssertAnalyzersValid(indexDefinition);

			indexes.AddOrUpdate(indexDefinition.Name, n =>
			{
				var directory = OpenOrCreateLuceneDirectory(indexDefinition, encodedName);
				return CreateIndexImplementation(encodedName, indexDefinition, directory);
			}, (s, index) => index);
		}

		private static void AssertAnalyzersValid(IndexDefinition indexDefinition)
		{
			foreach (var analyzer in from analyzer in indexDefinition.Analyzers
									 let analyzerType = typeof(StandardAnalyzer).Assembly.GetType(analyzer.Value) ?? Type.GetType(analyzer.Value, throwOnError: false)
									 where analyzerType == null
									 select analyzer)
			{
				throw new ArgumentException(string.Format("Could not create analyzer for field: '{0}' because the type '{1}' was not found", analyzer.Key, analyzer.Value));
			}
		}

		public Query GetLuceneQuery(string index, IndexQuery query, OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Query on non existing index {0}", index);
				throw new InvalidOperationException("Index '" + index + "' does not exists");
			}
			var fieldsToFetch = new FieldsToFetch(new string[0], AggregationOperation.None, null);
			return new Index.IndexQueryOperation(value, query, _ => false, fieldsToFetch, indexQueryTriggers).GetLuceneQuery();
		}

		public IEnumerable<IndexQueryResult> Query(
			string index,
			IndexQuery query,
			Func<IndexQueryResult, bool> shouldIncludeInResults,
			FieldsToFetch fieldsToFetch,
			OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Query on non existing index '{0}'", index);
				throw new InvalidOperationException("Index '" + index + "' does not exists");
			}

			var indexQueryOperation = new Index.IndexQueryOperation(value, query, shouldIncludeInResults, fieldsToFetch, indexQueryTriggers);
			if (query.Query != null && query.Query.Contains(Constants.IntersectSeparator))
				return indexQueryOperation.IntersectionQuery();
			return indexQueryOperation.Query();
		}

		public IEnumerable<RavenJObject> IndexEntires(
			string index,
			IndexQuery query,
			OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers,
			Reference<int> totalResults)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Query on non existing index '{0}'", index);
				throw new InvalidOperationException("Index '" + index + "' does not exists");
			}

			var indexQueryOperation = new Index.IndexQueryOperation(value, query, null, new FieldsToFetch(null,AggregationOperation.None, null), indexQueryTriggers);
			return indexQueryOperation.IndexEntries(totalResults);
		}

		protected internal static IDisposable EnsureInvariantCulture()
		{
			if (Thread.CurrentThread.CurrentCulture == CultureInfo.InvariantCulture)
				return null;

			var oldCurrentCulture = Thread.CurrentThread.CurrentCulture;
			var oldCurrentUiCulture = Thread.CurrentThread.CurrentUICulture;

			Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
			Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
			return new DisposableAction(() =>
			{
				Thread.CurrentThread.CurrentCulture = oldCurrentCulture;
				Thread.CurrentThread.CurrentUICulture = oldCurrentUiCulture;
			});
		}

		public void RemoveFromIndex(string index, string[] keys, WorkContext context)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Removing from non existing index '{0}', ignoring", index);
				return;
			}
			value.Remove(keys, context);
			context.RaiseIndexChangeNotification(new IndexChangeNotification
			{
				Name = index,
				Type = IndexChangeTypes.RemoveFromIndex
			});
		}

		public void Index(string index,
			AbstractViewGenerator viewGenerator,
			IndexingBatch batch,
			WorkContext context,
			IStorageActionsAccessor actions,
			DateTime minimumTimestamp)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Tried to index on a non existent index {0}, ignoring", index);
				return;
			}
			using (EnsureInvariantCulture())
			using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
			{
				value.IndexDocuments(viewGenerator, batch, actions, minimumTimestamp);
				context.RaiseIndexChangeNotification(new IndexChangeNotification
				{
					Name = index,
					Type = IndexChangeTypes.MapCompleted
				});
			}
		}

		public void Reduce(
			string index, 
			AbstractViewGenerator viewGenerator, 
			IEnumerable<IGrouping<int, object>> mappedResults,
			int level,
			WorkContext context, 
			IStorageActionsAccessor actions,
			HashSet<string> reduceKeys)
		{
			Index value;
			if (indexes.TryGetValue(index, out value) == false)
			{
				log.Debug("Tried to index on a non existent index {0}, ignoring", index);
				return;
			}
			var mapReduceIndex = value as MapReduceIndex;
			if (mapReduceIndex == null)
			{
				log.Warn("Tried to reduce on an index that is not a map/reduce index: {0}, ignoring", index);
				return;
			}
			using (EnsureInvariantCulture())
			{
				var reduceDocuments = new MapReduceIndex.ReduceDocuments(mapReduceIndex, viewGenerator, mappedResults, level, context, actions, reduceKeys);
				reduceDocuments.ExecuteReduction();
				context.RaiseIndexChangeNotification(new IndexChangeNotification
				{
					Name = index,
					Type = IndexChangeTypes.ReduceCompleted
				});
			}
		}

		public IDisposable GetCurrentIndexSearcher(string indexName, out IndexSearcher searcher)
		{
			return GetIndexByName(indexName).GetSearcher(out searcher);
		}

		public IDisposable GetCurrentIndexSearcherAndTermDocs(string indexName, out IndexSearcher searcher, out RavenJObject[] termsDocs)
		{
			return GetIndexByName(indexName).GetSearcherAndTermsDocs(out searcher, out termsDocs);
		}

		private Index GetIndexByName(string indexName)
		{
			var result = indexes.Where(index => String.Compare(index.Key, indexName, StringComparison.OrdinalIgnoreCase) == 0)
				.Select(x => x.Value)
				.FirstOrDefault();
			if (result == null)
				throw new InvalidOperationException(string.Format("Index '{0}' does not exist", indexName));
			return result;
		}

		public void RunIdleOperations()
		{
			foreach (var value in indexes.Values)
			{
				if ((SystemTime.UtcNow - value.LastIndexTime).TotalMinutes < 1)
					continue;

				value.Flush();
			}

			documentDatabase.TransactionalStorage.Batch(accessor =>
			{
				var maxDate = latestPersistedQueryTime;
				foreach (var index in indexes)
				{
					var lastQueryTime = index.Value.LastQueryTime ?? DateTime.MinValue;
					if (lastQueryTime <= latestPersistedQueryTime)
						continue;

					accessor.Lists.Set("Raven/Indexes/QueryTime", index.Key, new RavenJObject
					{
						{"LastQueryTime", lastQueryTime}
					}, UuidType.Indexing);

					if (lastQueryTime > maxDate)
						maxDate = lastQueryTime;
				}
				latestPersistedQueryTime = maxDate;
			});
		}

		public void FlushMapIndexes()
		{
			foreach (var value in indexes.Values.Where(value => !value.IsMapReduce))
			{
				value.Flush();
			}
		}

		public void FlushReduceIndexes()
		{
			foreach (var value in indexes.Values.Where(value => value.IsMapReduce))
			{
				value.Flush();
			}
		}

		public IIndexExtension GetIndexExtension(string index, string indexExtensionKey)
		{
			return GetIndexByName(index).GetExtension(indexExtensionKey);
		}

		public IIndexExtension GetIndexExtensionByPrefix(string index, string indexExtensionKeyPrefix)
		{
			return GetIndexByName(index).GetExtensionByPrefix(indexExtensionKeyPrefix);
		}

		public void SetIndexExtension(string indexName, string indexExtensionKey, IIndexExtension suggestionQueryIndexExtension)
		{
			GetIndexByName(indexName).SetExtension(indexExtensionKey, suggestionQueryIndexExtension);
		}

		public Index GetIndexInstance(string indexName)
		{
			return indexes.Where(index => String.Compare(index.Key, indexName, StringComparison.OrdinalIgnoreCase) == 0)
				.Select(x => x.Value)
				.FirstOrDefault();
		}

		public void MarkCachedQuery(string indexName)
		{
			GetIndexByName(indexName).MarkQueried();
		}

		public DateTime? GetLastQueryTime(string index)
		{
			return GetIndexByName(index).LastQueryTime;
		}

		public IndexingPerformanceStats[] GetIndexingPerformance(string index)
		{
			return GetIndexByName(index).GetIndexingPerformance();
		}

		public void Backup(string directory, string incrementalTag = null)
		{
			Parallel.ForEach(indexes.Values, index => 
				index.Backup(directory, path, incrementalTag));
		}

		public void MergeAllIndexes()
		{
			Parallel.ForEach(indexes.Values, index =>
			                                 index.MergeSegments());
		}
	}
}