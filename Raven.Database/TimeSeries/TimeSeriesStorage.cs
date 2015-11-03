using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Util;
using Raven.Database.Common;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Queries;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
using Raven.Database.TimeSeries.Notifications;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Voron;
using Voron.Trees;
using Voron.Trees.Fixed;
using Voron.Util;
using Voron.Util.Conversion;
using Constants = Raven.Abstractions.Data.Constants;
using Transaction = Voron.Impl.Transaction;

namespace Raven.Database.TimeSeries
{
	public class TimeSeriesStorage : IResourceStore, IDisposable
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		private volatile bool disposed;

		private readonly StorageEnvironment storageEnvironment;
		private readonly NotificationPublisher notificationPublisher;
		private readonly TimeSeriesMetricsManager metricsTimeSeries;

		public ReplicationTask ReplicationTask { get; }
		public int ReplicationTimeoutInMs { get; }

		public event Action TimeSeriesUpdated = () => { };

		internal const int AggregationPointStorageItemsLength = 6;

		public Guid ServerId { get; set; }

		public string TimeSeriesUrl { get; private set; }

		public string Name { get; private set; }
		public string ResourceName { get; private set; }
		public TransportState TransportState { get; private set; }
		public AtomicDictionary<object> ExtensionsState { get; private set; }
		public InMemoryRavenConfiguration Configuration { get; private set; }
		public DateTime LastWrite { get; set; }

		[CLSCompliant(false)]
		public TimeSeriesMetricsManager MetricsTimeSeries
		{
			get { return metricsTimeSeries; }
		}

		public TimeSeriesStorage(string serverUrl, string timeSeriesName, InMemoryRavenConfiguration configuration, TransportState receivedTransportState = null)
		{
			Name = timeSeriesName;
			TimeSeriesUrl = string.Format("{0}ts/{1}", serverUrl, timeSeriesName);
			ResourceName = string.Concat(Constants.TimeSeries.UrlPrefix, "/", timeSeriesName);

			metricsTimeSeries = new TimeSeriesMetricsManager();

			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.TimeSeries.DataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);
			TransportState = receivedTransportState ?? new TransportState();
			notificationPublisher = new NotificationPublisher(TransportState);
			ExtensionsState = new AtomicDictionary<object>();
			ReplicationTask = new ReplicationTask(this);
			ReplicationTimeoutInMs = configuration.Replication.ReplicationRequestTimeoutInMilliseconds;

			Configuration = configuration;
			Initialize();

			AppDomain.CurrentDomain.ProcessExit += ShouldDispose;
			AppDomain.CurrentDomain.DomainUnload += ShouldDispose;
		}

		private void Initialize()
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				storageEnvironment.CreateTree(tx, TreeNames.OpenLog);
				storageEnvironment.CreateTree(tx, TreeNames.CompressedLog);

				var metadata = storageEnvironment.CreateTree(tx, TreeNames.Metadata);
				var id = metadata.Read("id");
				if (id == null) // new db
				{
					ServerId = Guid.NewGuid();
					metadata.Add("id", ServerId.ToByteArray());
				}
				else
				{
					int used;
					ServerId = new Guid(id.Reader.ReadBytes(16, out used));
				}

				tx.Commit();
			}

			ReplicationTask.StartReplication();
		}

		private static StorageEnvironmentOptions CreateStorageOptionsFromConfiguration(string path, NameValueCollection settings)
		{
			bool result;
			if (bool.TryParse(settings[Constants.RunInMemory] ?? "false", out result) && result)
				return StorageEnvironmentOptions.CreateMemoryOnly();

			bool allowIncrementalBackupsSetting;
			if (bool.TryParse(settings[Constants.Voron.AllowIncrementalBackups] ?? "false", out allowIncrementalBackupsSetting) == false)
				throw new ArgumentException(Constants.Voron.AllowIncrementalBackups + " settings key contains invalid value");

			var directoryPath = path ?? AppDomain.CurrentDomain.BaseDirectory;
			var filePathFolder = new DirectoryInfo(directoryPath);
			if (filePathFolder.Exists == false)
				filePathFolder.Create();

			var tempPath = settings[Constants.Voron.TempPath];
			var journalPath = settings[Constants.RavenTxJournalPath];
			var options = StorageEnvironmentOptions.ForPath(directoryPath, tempPath, journalPath);
			options.IncrementalBackupEnabled = allowIncrementalBackupsSetting;
			return options;
		}

		public Reader CreateReader()
		{
			return new Reader(this);
		}

		public Writer CreateWriter()
		{
			LastWrite = SystemTime.UtcNow;
			return new Writer(this);
		}

		internal const string SeriesTreePrefix = "series-";
		internal const string PeriodTreePrefix = "periods-";
		internal const char PeriodsKeySeparator = '\uF8FF';
		internal const string TypesPrefix = "types-";
		internal const string StatsPrefix = "stats-";

		public class Reader : IDisposable
		{
			private readonly TimeSeriesStorage storage;
			private readonly Transaction tx;
			private readonly Tree metadata;
			private readonly TimeSeriesLogStorage logStorage;

			public Reader(TimeSeriesStorage storage)
			{
				this.storage = storage;
				tx = this.storage.storageEnvironment.NewTransaction(TransactionFlags.Read);
				metadata = tx.ReadTree(TreeNames.Metadata);
				logStorage = new TimeSeriesLogStorage(tx);
			}

			public IEnumerable<AggregatedPoint> GetAggregatedPoints(string typeName, string key, AggregationDuration duration, DateTimeOffset start, DateTimeOffset end, int skip = 0)
			{
				switch (duration.Type)
				{
					case AggregationDurationType.Seconds:
						if (start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by seconds, you cannot specify milliseconds");
						if (end.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by seconds, you cannot specify milliseconds");
						if (start.Second%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", duration.Duration, duration.Type));
						if (end.Second%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", duration.Duration, duration.Type));
						break;
					case AggregationDurationType.Minutes:
						if (start.Second != 0 || start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by minutes, you cannot specify seconds or milliseconds");
						if (end.Second != 0 || end.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by minutes, you cannot specify seconds or milliseconds");
						if (start.Minute%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", duration.Duration, duration.Type));
						if (end.Minute%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", duration.Duration, duration.Type));
						break;
					case AggregationDurationType.Hours:
						if (start.Minute != 0 || start.Second != 0 || start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify minutes, seconds or milliseconds");
						if (end.Minute != 0 || end.Second != 0 || end.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify minutes, seconds or milliseconds");
						if (start.Hour%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", duration.Duration, duration.Type));
						if (end.Hour%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", duration.Duration, duration.Type));
						break;
					case AggregationDurationType.Days:
						if (start.Hour != 0 || start.Minute != 0 || start.Second != 0 || start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify hours, minutes, seconds or milliseconds");
						if (end.Hour != 0 || end.Minute != 0 || end.Second != 0 || end.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify hours, minutes, seconds or milliseconds");
						if (start.Day%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", duration.Duration, duration.Type));
						if (end.Day%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", duration.Duration, duration.Type));
						break;
					case AggregationDurationType.Months:
						if (start.Day != 1 || start.Hour != 0 || start.Minute != 0 || start.Second != 0 || start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify days, hours, minutes, seconds or milliseconds");
						if (end.Day != 1 || end.Minute != 0 || end.Second != 0 || end.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify days, hours, minutes, seconds or milliseconds");
						if (start.Month%(duration.Duration) != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", duration.Duration, duration.Type));
						if (end.Month%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", duration.Duration, duration.Type));
						break;
					case AggregationDurationType.Years:
						if (start.Month != 1 || start.Day != 1 || start.Hour != 0 || start.Minute != 0 || start.Second != 0 || start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by years, you cannot specify months, days, hours, minutes, seconds or milliseconds");
						if (end.Month != 1 || end.Day != 1 || end.Minute != 0 || end.Second != 0 || end.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by years, you cannot specify months, days, hours, minutes, seconds or milliseconds");
						if (start.Year%(duration.Duration) != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", duration.Duration, duration.Type));
						if (end.Year%duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", duration.Duration, duration.Type));
						break;
					default:
						throw new ArgumentOutOfRangeException();
				}

				if (string.IsNullOrWhiteSpace(typeName))
					throw new InvalidOperationException("Type cannot be empty");

				var tree = tx.ReadTree(SeriesTreePrefix + typeName);
				if (tree == null)
					yield break;

				var type = storage.GetTimeSeriesType(metadata, typeName);
				if (type == null)
					throw new InvalidOperationException("Type does not exist");

				using (var periodTx = storage.storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
				{
					var fixedTree = tree.FixedTreeFor(key, (byte)(type.Fields.Length * sizeof(double)));
					var treeName = PeriodTreePrefix + typeName;
					var periodFixedTree = (periodTx.ReadTree(treeName) ?? storage.storageEnvironment.CreateTree(periodTx, treeName))
						.FixedTreeFor(key + PeriodsKeySeparator + duration.Type + "-" + duration.Duration, (byte)(type.Fields.Length * AggregationPointStorageItemsLength * sizeof(double)));

					using (var periodWriter = new RollupWriter(periodFixedTree, type.Fields.Length))
					{
						using (var periodTreeIterator = periodFixedTree.Iterate())
						using (var rawTreeIterator = fixedTree.Iterate())
						{
							foreach (var range in GetAggregatedPointsRanges(key, start, end, duration, type.Fields.Length))
							{
								// seek period tree iterator, if found exact match!!, add and move to the next
								var startTicks = range.StartAt.UtcTicks;
								if (periodTreeIterator.Seek(startTicks))
								{
									if (periodTreeIterator.CurrentKey == startTicks)
									{
										var valueReader = periodTreeIterator.CreateReaderForCurrent();
										int used;
										var bytes = valueReader.ReadBytes(type.Fields.Length * AggregationPointStorageItemsLength * sizeof(double), out used);
										Debug.Assert(used == type.Fields.Length*AggregationPointStorageItemsLength*sizeof (double));

										for (int i = 0; i < type.Fields.Length; i++)
										{
											var startPosition = i * AggregationPointStorageItemsLength;
											range.Values[i].Volume = EndianBitConverter.Big.ToDouble(bytes, (startPosition + 0) * sizeof(double));
											if (range.Values[i].Volume != 0)
											{
												range.Values[i].High = EndianBitConverter.Big.ToDouble(bytes, (startPosition + 1)*sizeof (double));
												range.Values[i].Low = EndianBitConverter.Big.ToDouble(bytes, (startPosition + 2)*sizeof (double));
												range.Values[i].Open = EndianBitConverter.Big.ToDouble(bytes, (startPosition + 3)*sizeof (double));
												range.Values[i].Close = EndianBitConverter.Big.ToDouble(bytes, (startPosition + 4)*sizeof (double));
												range.Values[i].Sum = EndianBitConverter.Big.ToDouble(bytes, (startPosition + 5)*sizeof (double));
											}
										}
										yield return range;
										continue;
									}
								}

								// seek tree iterator, if found note don't go to next range!!, sum, add to period tree, move next
								if (range.StartAt.Minute == 0 && range.StartAt.Second == 0)
								{
									
								}
								if (rawTreeIterator.Seek(startTicks))
								{
									GetAllPointsForRange(rawTreeIterator, range, type.Fields.Length);
								}

								// if not found, create empty periods until the end or the next valid period
								/*if (range.Volume == 0)
								{
								}*/

								periodWriter.Append(range.StartAt, range);
								yield return range;
							}
						}
					}
					periodTx.Commit();
				}
			}

			private void GetAllPointsForRange(FixedSizeTree.IFixedSizeIterator rawTreeIterator, AggregatedPoint aggregatedPoint, int valueLength)
			{
				var endTicks = aggregatedPoint.Duration.AddToDateTime(aggregatedPoint.StartAt).Ticks;
				var buffer = new byte[sizeof(double) * valueLength];
				var firstPoint = true;

				do
				{
					var ticks = rawTreeIterator.CurrentKey;
					if (ticks >= endTicks)
						return;

					var point = new TimeSeriesPoint
					{
#if DEBUG
						DebugKey = aggregatedPoint.DebugKey,
#endif
						At = new DateTimeOffset(ticks, TimeSpan.Zero),
						Values = new double[valueLength],
					};

					var reader = rawTreeIterator.CreateReaderForCurrent();
					reader.Read(buffer, 0, sizeof(double) * valueLength);

					for (int i = 0; i < valueLength; i++)
					{
						var value = point.Values[i] = EndianBitConverter.Big.ToDouble(buffer, i * sizeof(double));

						if (firstPoint)
						{
							aggregatedPoint.Values[i].Open = aggregatedPoint.Values[i].High = aggregatedPoint.Values[i].Low = aggregatedPoint.Values[i].Sum = value;
							aggregatedPoint.Values[i].Volume = 1;
						}
						else
						{
							aggregatedPoint.Values[i].High = Math.Max(aggregatedPoint.Values[i].High, value);
							aggregatedPoint.Values[i].Low = Math.Min(aggregatedPoint.Values[i].Low, value);
							aggregatedPoint.Values[i].Sum += value;
							aggregatedPoint.Values[i].Volume += 1;
						}

						aggregatedPoint.Values[i].Close = value;

					}
					firstPoint = false;

				} while (rawTreeIterator.MoveNext());
			}

			private IEnumerable<AggregatedPoint> GetAggregatedPointsRanges(string key, DateTimeOffset start, DateTimeOffset end, AggregationDuration duration, int valueLength)
			{
				var startAt = start;
				while (true)
				{
					var nextStartAt = duration.AddToDateTime(startAt);
					if (startAt == end)
						yield break;
					if (nextStartAt > end)
					{
						throw new InvalidOperationException("Debug: Duration is not aligned with the end of the range.");
					}
					var rangeValues = new AggregatedPoint.AggregationValue[valueLength];
					for (int i = 0; i < valueLength; i++)
					{
						rangeValues[i] = new AggregatedPoint.AggregationValue();
					}
					yield return new AggregatedPoint
					{
#if DEBUG
						DebugKey = key,
#endif
						StartAt = startAt,
						Duration = duration,
						Values = rangeValues,
					};
					startAt = nextStartAt;
				}
			}

			public void Dispose()
			{
				if (tx != null)
					tx.Dispose();
			}

			public IEnumerable<TimeSeriesPoint> GetPoints(string typeName, string key, DateTimeOffset start, DateTimeOffset end, int skip = 0)
			{
				var tree = tx.ReadTree(SeriesTreePrefix + typeName);
				if (tree == null)
					yield break;

				var type = storage.GetTimeSeriesType(metadata, typeName);
				if (type == null)
					throw new InvalidOperationException("Type does not exist");
				var buffer = new byte[type.Fields.Length * sizeof(double)];

				var fixedTree = tree.FixedTreeFor(key, (byte) (type.Fields.Length*sizeof (double)));
				using (var fixedIt = fixedTree.Iterate())
				{
					if (fixedIt.Seek(start.UtcTicks) == false || 
						(skip != 0 && fixedIt.Skip(skip) == false))
						yield break;

					do
					{
						var currentKey = fixedIt.CurrentKey;
						if (currentKey > end.UtcTicks)
							yield break;

						var point = new TimeSeriesPoint
						{
#if DEBUG
							DebugKey = fixedTree.Name.ToString(),
#endif
							At = new DateTimeOffset(currentKey, TimeSpan.Zero),
							Values = new double[type.Fields.Length],
						};

						var reader = fixedIt.CreateReaderForCurrent();
						reader.Read(buffer, 0, type.Fields.Length * sizeof(double));
						for (int i = 0; i < type.Fields.Length; i++)
						{
							point.Values[i] = EndianBitConverter.Big.ToDouble(buffer, i * sizeof(double));
						}

						yield return point;
					} while (fixedIt.MoveNext());
				}
			}

			public TimeSeriesKeySummary GetKey(string typeName, string key)
			{
				var type = storage.GetTimeSeriesType(metadata, typeName);
				if (type == null)
					return null;

				var tree = tx.ReadTree(SeriesTreePrefix + typeName);
				var fixedTree = tree.FixedTreeFor(key, (byte) (type.Fields.Length*sizeof (double)));
				var keySummary = new TimeSeriesKeySummary
				{
					Type = type,
					Key = key,
					PointsCount = fixedTree.NumberOfEntries,
				};
				using (var fixedIt = fixedTree.Iterate())
				{
					if (fixedIt.Seek(DateTimeOffset.MinValue.Ticks))
						keySummary.MinPoint = new DateTimeOffset(fixedIt.CurrentKey, TimeSpan.Zero);
					if (fixedIt.SeekToLast())
						keySummary.MaxPoint = new DateTimeOffset(fixedIt.CurrentKey, TimeSpan.Zero);
				}
				return keySummary;
			}

			public IEnumerable<TimeSeriesKey> GetKeys(string typeName, int skip)
			{
				var type = storage.GetTimeSeriesType(metadata, typeName);
				if (type == null)
					yield break;

				// Can happend if the type doesn't have any keys
				var tree = tx.ReadTree(SeriesTreePrefix + typeName);
				if (tree == null)
					yield break;

				using (var it = tree.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) && (skip == 0 || it.Skip(skip)))
					{
						do
						{
							var key = it.CurrentKey.ToString();
							var fixedTree = tree.FixedTreeFor(key, (byte) (type.Fields.Length*sizeof (double)));
							yield return new TimeSeriesKey
							{
								Type = type,
								Key = key,
								PointsCount = fixedTree.NumberOfEntries,
							};
						} while (it.MoveNext());
					}
				}
			}

			public IEnumerable<TimeSeriesType> GetTypes(int skip)
			{
				using (var it = metadata.Iterate())
				{
					it.RequiredPrefix = TypesPrefix;
					if (it.Seek(it.RequiredPrefix) && (skip == 0 || it.Skip(skip)))
					{
						do
						{
							var typeTreeName = it.CurrentKey.ToString();
							var typeName = typeTreeName.Replace(TypesPrefix, "");
                            var type = storage.ReadTypeInternal(it.CreateReaderForCurrent());
							var tree = tx.ReadTree(SeriesTreePrefix + typeName);
							type.KeysCount = tree == null ? 0 : tree.State.EntriesCount;
							yield return type;
						} while (it.MoveNext());
					}
				}
			}

			public long GetTypesCount()
			{
				var val = metadata.Read(StatsPrefix + "types-count");
				if (val == null)
					return 0;
				var count = val.Reader.ReadLittleEndianInt64();
				return count;
			}

			public long GetKeysCount()
			{
				var val = metadata.Read(StatsPrefix + "keys-count");
				if (val == null)
					return 0;
				var count = val.Reader.ReadLittleEndianInt64();
				return count;
			}

			public long GetPointsCount()
			{
				var val = metadata.Read(StatsPrefix + "points-count");
				if (val == null)
					return 0;
				var count = val.Reader.ReadLittleEndianInt64();
				return count;
			}

			/*[Conditional("DEBUG")]
			public void CalculateKeysAndPointsCount()
			{
				using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
				{
					long keys = 0, points = 0;
					using (var rootIt = tx.Root.Iterate())
					{
						rootIt.RequiredPrefix = SeriesTreePrefix;
						if (rootIt.Seek(rootIt.RequiredPrefix))
						{
							do
							{
								var typeTreeName = rootIt.CurrentKey.ToString();
								var type = GetTimeSeriesType(tx, typeTreeName.Replace(SeriesTreePrefix, ""));
								var tree = tx.ReadTree(typeTreeName);
								using (var it = tree.Iterate())
								{
									if (it.Seek(Slice.BeforeAllKeys))
									{
										do
										{
											keys++;
											var fixedTree = tree.FixedTreeFor(it.CurrentKey.ToString(), (byte)(type.Fields.Length * sizeof(double)));
											points += fixedTree.NumberOfEntries;
										} while (it.MoveNext());
									}
								}
							} while (rootIt.MoveNext());
						}
					}

					var val = new byte[sizeof(long)];
					EndianBitConverter.Little.CopyBytes(keys, val, 0);
					var metadata = tx.ReadTree("$metadata");
					metadata.Add(new Slice(StatsPrefix + "keys-count"), new Slice(val));
					EndianBitConverter.Little.CopyBytes(points, val, 0);
					metadata.Add(new Slice(StatsPrefix + "points-count"), new Slice(val));
				}
			}

			/// <summary>
			/// This intended to be here for testing and debugging. This code is not reachable in production, since we always cache the stats.
			/// </summary>
			private long CalculateTypesCount()
			{
				long count = 0;
				using (var it = metadata.Iterate())
				{
					it.RequiredPrefix = TypesPrefix;
					if (it.Seek(it.RequiredPrefix))
					{
						do
						{
							count++;
						} while (it.MoveNext());
					}
				}

				// var val = new byte[sizeof (long)];
				// EndianBitConverter.Little.CopyBytes(count, val, 0);
				// metadata.Add(new Slice(StatsPrefix + "types-count"), new Slice(val));

				return count;
			}*/

			public TimeSeriesReplicationDocument GetReplicationData()
			{
				var readResult = metadata.Read("replication");
				if (readResult == null)
					return null;

				var stream = readResult.Reader.AsStream();
				stream.Position = 0;
				using (var streamReader = new StreamReader(stream))
				using (var jsonTextReader = new JsonTextReader(streamReader))
				{
					return new JsonSerializer().Deserialize<TimeSeriesReplicationDocument>(jsonTextReader);
				}
			}

			public long GetLastEtag()
			{
				return new TimeSeriesLogStorage(tx).GetLastEtag();
			}

			public IEnumerable<ReplicationLogItem> GetLogsSinceEtag(long etag)
			{
				return logStorage.GetLogsSinceEtag(etag);
			}
		}

		public class Writer : IDisposable
		{
			private readonly TimeSeriesStorage storage;
			private readonly Transaction tx;

			private Tree tree;
			private readonly Dictionary<string, AggregationRange> rollupsToClear = new Dictionary<string, AggregationRange>();
			private TimeSeriesType currentType;
			private readonly Dictionary<byte, byte[]> valBuffers = new Dictionary<byte, byte[]>();
			private readonly TimeSeriesLogStorage logStorage;
			private readonly Tree metadata;

			public JsonSerializer JsonSerializer { get; set; }


			public Writer(TimeSeriesStorage storage)
			{
				this.storage = storage;
				JsonSerializer = new JsonSerializer();
				currentType = new TimeSeriesType();
				tx = storage.storageEnvironment.NewTransaction(TransactionFlags.ReadWrite);
				logStorage = new TimeSeriesLogStorage(tx);
				metadata = tx.ReadTree(TreeNames.Metadata);
			}

			public bool Append(string type, string key, DateTimeOffset time, params double[] values)
			{
				if (string.IsNullOrWhiteSpace(type))
					throw new ArgumentOutOfRangeException("type", "Type cannot be empty");
				if (string.IsNullOrWhiteSpace(key))
					throw new ArgumentOutOfRangeException("key", "Key cannot be empty");

				if (currentType.Type != type)
				{
					currentType = storage.GetTimeSeriesType(metadata, type);
					if (currentType == null)
						throw new InvalidOperationException("There is no type named: " + type);

					var treeName = SeriesTreePrefix + currentType.Type;
					tree = tx.ReadTree(treeName) ?? storage.storageEnvironment.CreateTree(tx, treeName);
				}

				if (values.Length != currentType.Fields.Length)
					throw new ArgumentOutOfRangeException("values", string.Format("Appended values should be the same length the series values length which is {0} and not {1}", currentType.Fields.Length, values.Length));

				AggregationRange range;
				var clearKey = type + PeriodsKeySeparator + key;
				if (rollupsToClear.TryGetValue(clearKey, out range))
				{
					if (time > range.End)
					{
						range.End = time;
					}
					else if (time < range.Start)
					{
						range.Start = time;
					}
				}
				else
				{
					rollupsToClear.Add(clearKey, new AggregationRange(type, key, time));
				}

				var bufferSize = (byte) (currentType.Fields.Length*sizeof (double));
				byte[] valBuffer;
				if (valBuffers.TryGetValue(bufferSize, out valBuffer) == false)
				{
					valBuffers[bufferSize] = valBuffer = new byte[bufferSize];
				}
				for (int i = 0; i < values.Length; i++)
				{
					EndianBitConverter.Big.CopyBytes(values[i], valBuffer, i*sizeof (double));
				}

				var fixedTree = tree.FixedTreeFor(key, bufferSize);
				if (fixedTree.NumberOfEntries == 0)
				{
					UpdateKeysCount(1);
				}
				var utcTicks = time.UtcTicks;
				var newPointWasAppended = fixedTree.Add(utcTicks, valBuffer);
				if (newPointWasAppended)
				{
					UpdatePointsCount(1);
				}
				logStorage.Append(type, key, utcTicks, values);
				return newPointWasAppended;
			}

			private void UpdateKeysCount(long delta)
			{
				metadata.Increment(StatsPrefix + "keys-count", delta);
			}

			private void UpdatePointsCount(long delta)
			{
				metadata.Increment(StatsPrefix + "points-count", delta);
			}

			private void UpdateTypesCount(long delta)
			{
				metadata.Increment(StatsPrefix + "types-count", delta);
			}

			public void Dispose()
			{
				if (tx != null)
					tx.Dispose();
			}

			public void Commit()
			{
				foreach (var rollupRange in rollupsToClear.Values)
				{
					DeleteRangeInRollups(rollupRange.Type, rollupRange.Key, rollupRange.Start, rollupRange.End);
				}
				tx.Commit();
				storage.LastWrite = SystemTime.UtcNow;
				storage.TimeSeriesUpdated();
			}

			public void DeleteRangeInRollups(string typeName, string key, DateTimeOffset start, DateTimeOffset end)
			{
				var periodTree = tx.ReadTree(PeriodTreePrefix + typeName);
				if (periodTree == null)
					return;

				using (var it = periodTree.Iterate())
				{
					it.RequiredPrefix = key + PeriodsKeySeparator;
					if (it.Seek(it.RequiredPrefix) == false)
						return;

					var type = storage.GetTimeSeriesType(metadata, typeName);
					if (type == null)
						throw new InvalidOperationException("There is no type named: " + typeName);

					do
					{
						var periodTreeName = it.CurrentKey.ToString();
						var periodFixedTree = periodTree.FixedTreeFor(periodTreeName, (byte) (type.Fields.Length*AggregationPointStorageItemsLength*sizeof (double)));
						if (periodFixedTree == null)
							continue;

						var duration = GetDurationFromTreeName(periodTreeName);
						using (var fixedIt = periodFixedTree.Iterate())
						{
							if (fixedIt.Seek(duration.GetStartOfRangeForDateTime(start).UtcTicks) == false)
								continue;

							do
							{
								var currentKey = fixedIt.CurrentKey;
								if (currentKey > duration.GetStartOfRangeForDateTime(end).UtcTicks)
									break;

								periodFixedTree.Delete(currentKey);
							} while (fixedIt.MoveNext());
						}
					} while (it.MoveNext());
				}
			}

			private AggregationDuration GetDurationFromTreeName(string periodTreeName)
			{
				var separatorIndex = periodTreeName.LastIndexOf(PeriodsKeySeparator);
				var s = periodTreeName.Substring(separatorIndex + 1);
				var strings = s.Split('-');
				return new AggregationDuration(GenericUtil.ParseEnum<AggregationDurationType>(strings[0]), int.Parse(strings[1]));
			}

			public long DeleteKey(string typeName, string key)
			{
				var type = storage.GetTimeSeriesType(metadata, typeName);
				if (type == null)
					throw new InvalidOperationException("There is no type named: " + typeName);

				tree = tx.ReadTree(SeriesTreePrefix + typeName);
				if (tree == null)
					throw new InvalidOperationException("There is no type named: " + typeName);

				var numberOfEntriesRemoved = tree.DeleteFixedTreeFor(key, (byte)(type.Fields.Length * sizeof(double)));
				UpdatePointsCount(-numberOfEntriesRemoved);
				UpdateKeysCount(-1);

				Debug.Assert(numberOfEntriesRemoved > 0, "It isn't possible that we have no points, as we check for it at the beginning of the method");
				logStorage.DeleteKey(typeName, key);

				return numberOfEntriesRemoved;
			}

			public bool DeletePoint(TimeSeriesPointId point)
			{
				tree = tx.ReadTree(SeriesTreePrefix + point.Type);
				if (tree == null)
					return false;

				var type = storage.GetTimeSeriesType(metadata, point.Type);
				if (type == null)
					throw new InvalidOperationException("There is no type named: " + point.Type);

				var fixedTree = tree.FixedTreeFor(point.Key, (byte)(type.Fields.Length * sizeof(double)));
				var atTicks = point.At.UtcTicks;
				var result = fixedTree.Delete(atTicks);
				if (result.NumberOfEntriesDeleted > 0)
				{
					UpdatePointsCount(-1);
				}
				if (result.TreeRemoved)
				{
					UpdateKeysCount(-1);
				}

				Debug.Assert(result.NumberOfEntriesDeleted > 0 || result.TreeRemoved == false, "It isn't possible that we remove a tree without removing a point");
				logStorage.DeletePoint(point.Type, point.Key, atTicks);

				return true;
			}

			public void DeleteKeyInRollups(string type, string key)
			{
				var periodTree = tx.ReadTree(PeriodTreePrefix + type);
				if (periodTree == null)
					return;
				
				// TODO: Implement better: we cannot delete and continue the iteration without a seek
				throw new NotImplementedException();
				using (var it = periodTree.Iterate())
				{
					it.RequiredPrefix = key + PeriodsKeySeparator;
					if (it.Seek(it.RequiredPrefix))
					{
						periodTree.Delete(it.CurrentKey);
					}
				}
			}

			public void DeletePointInRollups(TimeSeriesPointId point)
			{
				var periodTree = tx.ReadTree(PeriodTreePrefix + point.Type);
				if (periodTree == null)
					return
						;

				// TODO: Implement better: we cannot delete and continue the iteration without a seek
				throw new NotImplementedException();
			}

			public void DeleteRange(string typeName, string key, long start, long end)
			{
				var type = storage.GetTimeSeriesType(metadata, typeName);
				if (type == null)
					throw new InvalidOperationException("There is no type named: " + typeName);

				tree = tx.ReadTree(SeriesTreePrefix + typeName);
				if (tree == null)
					throw new InvalidOperationException("There is no type named: " + typeName);

				var fixedTree = tree.FixedTreeFor(key, (byte)(type.Fields.Length * sizeof(double)));
				var result = fixedTree.DeleteRange(start, end);
				if (result.NumberOfEntriesDeleted > 0)
				{
					UpdatePointsCount(-result.NumberOfEntriesDeleted);
				}
				if (result.TreeRemoved)
				{
					UpdateKeysCount(-1);
				}

				Debug.Assert(result.NumberOfEntriesDeleted > 0 || result.TreeRemoved == false, "It isn't possible that we remove a tree without removing a point");
				logStorage.DeleteRange(typeName, key, start, end);
			}

			public void CreateType(string type, string[] fields)
			{
				DoCreateType(type, fields);
				logStorage.CreateType(type, fields);
			}

			internal void DoCreateType(string type, string[] fields)
			{
				if (string.IsNullOrWhiteSpace(type))
					throw new InvalidOperationException("Type cannot be empty");

				if (fields.Length < 1)
					throw new InvalidOperationException("Fields length should be equal or greater than 1");

				if (fields.Any(string.IsNullOrWhiteSpace))
					throw new InvalidOperationException("Field name cannot be empty.");

				var existingType = GetTimeSeriesType(type);
				if (existingType != null && existingType.KeysCount > 0)
				{
					var message = string.Format("There an existing type with the same name but with different fields. " +
												"Since the type has already {2} keys, the replication failed to overwrite this type. " +
												"{0} <=> {1}", string.Join(",", existingType.Fields), string.Join(",", fields), existingType.KeysCount);
					throw new InvalidOperationException(message);
				}

				using (var ms = new MemoryStream())
				using (var sw = new StreamWriter(ms))
				using (var jsonTextWriter = new JsonTextWriter(sw))
				{
					JsonSerializer.Serialize(jsonTextWriter, new TimeSeriesType
					{
						Type = type,
						Fields = fields,
					});
					sw.Flush();
					ms.Position = 0;
					metadata.Add(TypesPrefix + type, ms);
				}

				if (existingType == null) // A new type, not an overwritten type
				{
					UpdateTypesCount(1);
				}
			}

			public void DeleteType(string type)
			{
				DoDeleteType(type);
				logStorage.DeleteType(type);
			}

			public void DoDeleteType(string type)
			{
				var existingType = GetTimeSeriesType(type);
				if (existingType == null)
					throw new InvalidOperationException(string.Format("Type {0} does not exist", type));

				if (existingType.KeysCount > 0)
					throw new InvalidOperationException(string.Format("Cannot delete type '{0}' because it has {1} associated key{2}.", type, existingType.KeysCount, existingType.KeysCount == 1 ? "" : "s"));

				metadata.Delete(TypesPrefix + type);
				UpdateTypesCount(-1);
			}

			public void UpdateReplications(TimeSeriesReplicationDocument newReplicationDocument)
			{
				using (var memoryStream = new MemoryStream())
				using (var streamWriter = new StreamWriter(memoryStream))
				using (var jsonTextWriter = new JsonTextWriter(streamWriter))
				{
					JsonSerializer.Serialize(jsonTextWriter, newReplicationDocument);
					streamWriter.Flush();
					memoryStream.Position = 0;

					metadata.Add("replication", memoryStream);
				}

				storage.ReplicationTask.SignalUpdate();
			}

			public void PostReplicationLogItem(ReplicationLogItem logItem)
			{
				logStorage.PostReplicationLogItem(logItem, this);
			}

			public TimeSeriesType GetTimeSeriesType(string type)
			{
				var timeSeriesType = storage.GetTimeSeriesType(metadata, type);
				if (timeSeriesType == null)
					return null;

				var seriesTree = tx.ReadTree(SeriesTreePrefix + type);
				if (seriesTree != null)
				{
					timeSeriesType.KeysCount = seriesTree.State.EntriesCount;
				}
				return timeSeriesType;
			}
		}

		public class RollupWriter : IDisposable
		{
			private readonly FixedSizeTree tree;
			private readonly int numberOfValues;
			private readonly byte[] valBuffer;

			public RollupWriter(FixedSizeTree tree, int numberOfValues)
			{
				this.tree = tree;
				this.numberOfValues = numberOfValues;
				valBuffer = new byte[numberOfValues * AggregationPointStorageItemsLength * sizeof(double)];
			}

			public void Append(DateTimeOffset time, AggregatedPoint aggregatedPoint)
			{
				for (int i = 0; i < numberOfValues; i++)
				{
					var rangeValue = aggregatedPoint.Values[i];
					var startPosition = i * AggregationPointStorageItemsLength;
					EndianBitConverter.Big.CopyBytes(rangeValue.Volume, valBuffer, (startPosition + 0) * sizeof(double));
					// if (rangeValue.Volume != 0)
					{
						EndianBitConverter.Big.CopyBytes(rangeValue.High, valBuffer, (startPosition + 1) * sizeof(double));
						EndianBitConverter.Big.CopyBytes(rangeValue.Low, valBuffer, (startPosition + 2) * sizeof(double));
						EndianBitConverter.Big.CopyBytes(rangeValue.Open, valBuffer, (startPosition + 3) * sizeof(double));
						EndianBitConverter.Big.CopyBytes(rangeValue.Close, valBuffer, (startPosition + 4) * sizeof(double));
						EndianBitConverter.Big.CopyBytes(rangeValue.Sum, valBuffer, (startPosition + 5) * sizeof(double));
					}
				}

				tree.Add(time.UtcTicks, valBuffer);
			}

			public void Dispose()
			{
			}
		}

		public void Dispose()
		{
			if (disposed)
				return;

			// give it 3 seconds to complete requests
			for (int i = 0; i < 30 && Interlocked.Read(ref metricsTimeSeries.ConcurrentRequestsCount) > 0; i++)
			{
				Thread.Sleep(100);
			}

			AppDomain.CurrentDomain.ProcessExit -= ShouldDispose;
			AppDomain.CurrentDomain.DomainUnload -= ShouldDispose;

			disposed = true;

			var exceptionAggregator = new ExceptionAggregator(Log, "Could not properly dispose TimeSeriesStorage");

			if (storageEnvironment != null)
				exceptionAggregator.Execute(storageEnvironment.Dispose);

			if (metricsTimeSeries != null)
				exceptionAggregator.Execute(metricsTimeSeries.Dispose);

			exceptionAggregator.ThrowIfNeeded();
		}

		private void ShouldDispose(object sender, EventArgs eventArgs)
		{
			Dispose();
		}

		public TimeSeriesMetrics CreateMetrics()
		{
			var metrics = metricsTimeSeries;

			return new TimeSeriesMetrics
			{
				RequestsPerSecond = Math.Round(metrics.RequestsPerSecondCounter.CurrentValue, 3),
				Appends = metrics.Appends.CreateMeterData(),
				Deletes = metrics.Deletes.CreateMeterData(),
				ClientRequests = metrics.ClientRequests.CreateMeterData(),
				IncomingReplications = metrics.IncomingReplications.CreateMeterData(),
				OutgoingReplications = metrics.OutgoingReplications.CreateMeterData(),

				RequestsDuration = metrics.RequestDurationMetric.CreateHistogramData(),

				ReplicationBatchSizeMeter = metrics.ReplicationBatchSizeMeter.ToMeterDataDictionary(),
				ReplicationBatchSizeHistogram = metrics.ReplicationBatchSizeHistogram.ToHistogramDataDictionary(),
				ReplicationDurationHistogram = metrics.ReplicationDurationHistogram.ToHistogramDataDictionary(),
			};
		}

		public TimeSeriesStats CreateStats(Reader reader)
		{
			var stats = new TimeSeriesStats
			{
				Name = Name,
				Url = TimeSeriesUrl,
				TypesCount = reader.GetTypesCount(),
				KeysCount = reader.GetKeysCount(),
				PointsCount = reader.GetPointsCount(),
				LastEtag = reader.GetLastEtag(),
				ReplicationTasksCount = ReplicationTask.GetActiveTasksCount(),
				ReplicatedServersCount = 0, //TODO: get the correct number
				TimeSeriesSize = SizeHelper.Humane(TimeSeriesEnvironment.Stats().UsedDataFileSizeInBytes),
				RequestsPerSecond = Math.Round(metricsTimeSeries.RequestsPerSecondCounter.CurrentValue, 3),
			};
			return stats;
		}

		public NotificationPublisher Publisher
		{
			get { return notificationPublisher; }
		}

		public StorageEnvironment TimeSeriesEnvironment
		{
			get { return storageEnvironment; }
		}

		private TimeSeriesType GetTimeSeriesType(Tree metadata, string type)
		{
			var readResult = metadata.Read(TypesPrefix + type);
			if (readResult == null)
				return null;

			return ReadTypeInternal(readResult.Reader);
		}

		private TimeSeriesType ReadTypeInternal(ValueReader reader)
		{
			using (var stream = reader.AsStream())
			{
				stream.Position = 0;
				using (var streamReader = new StreamReader(stream))
				using (var jsonTextReader = new JsonTextReader(streamReader))
				{
					return new JsonSerializer().Deserialize<TimeSeriesType>(jsonTextReader);
				}
			}
		}

		public static class TreeNames
		{
			public const string Metadata = "$metadata";
			public const string CompressedLog = "CompressedLog";
			public const string OpenLog = "OpenLog";
		}
	}
}