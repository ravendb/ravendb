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
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
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

		public Guid ServerId { get; set; }

		public string TimeSeriesUrl { get; private set; }

		public string Name { get; private set; }
		public string ResourceName { get; private set; }
		public TransportState TransportState { get; private set; }
		public AtomicDictionary<object> ExtensionsState { get; private set; }
		public InMemoryRavenConfiguration Configuration { get; private set; }
		public DateTime LastWrite { get; set; }

		public JsonSerializer JsonSerializer { get; set; }

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

			JsonSerializer = new JsonSerializer();
			storageEnvironment = new StorageEnvironment(options);
			TransportState = receivedTransportState ?? new TransportState();
			notificationPublisher = new NotificationPublisher(TransportState);
			ExtensionsState = new AtomicDictionary<object>();

			Configuration = configuration;
			Initialize();

			AppDomain.CurrentDomain.ProcessExit += ShouldDispose;
			AppDomain.CurrentDomain.DomainUnload += ShouldDispose;
		}

		private void Initialize()
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var metadata = storageEnvironment.CreateTree(tx, "$metadata");
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

		public class Point
		{
#if DEBUG
			public string DebugKey { get; set; }
#endif
			public DateTime At { get; set; }

			public double[] Values { get; set; }
			
			public double Value
			{
				get { return Values[0]; }
			}
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

			public Reader(TimeSeriesStorage storage)
			{
				this.storage = storage;
				tx = this.storage.storageEnvironment.NewTransaction(TransactionFlags.Read);
			}

			public IEnumerable<Point> Query(TimeSeriesQuery query)
			{
				return GetRawQueryResult(query);
			}

			public IEnumerable<Point>[] Query(params TimeSeriesQuery[] queries)
			{
				var result = new IEnumerable<Point>[queries.Length];
				for (int i = 0; i < queries.Length; i++)
				{
					result[i] = GetRawQueryResult(queries[i]);
				}
				return result;
			}

			public IEnumerable<Range> QueryRollup(TimeSeriesRollupQuery query)
			{
				return GetQueryRollup(query);
			}

			public IEnumerable<Range>[] QueryRollup(params TimeSeriesRollupQuery[] queries)
			{
				var result = new IEnumerable<Range>[queries.Length];
				for (int i = 0; i < queries.Length; i++)
				{
					result[i] = GetQueryRollup(queries[i]);
				}
				return result;
			}

			private IEnumerable<Range> GetQueryRollup(TimeSeriesRollupQuery query)
			{
				switch (query.Duration.Type)
				{
					case PeriodType.Seconds:
						if (query.Start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by seconds, you cannot specify milliseconds");
						if (query.End.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by seconds, you cannot specify milliseconds");
						if (query.Start.Second%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", query.Duration.Duration, query.Duration.Type));
						if (query.End.Second%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", query.Duration.Duration, query.Duration.Type));
						break;
					case PeriodType.Minutes:
						if (query.Start.Second != 0 || query.Start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by minutes, you cannot specify seconds or milliseconds");
						if (query.End.Second != 0 || query.End.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by minutes, you cannot specify seconds or milliseconds");
						if (query.Start.Minute%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", query.Duration.Duration, query.Duration.Type));
						if (query.End.Minute%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", query.Duration.Duration, query.Duration.Type));
						break;
					case PeriodType.Hours:
						if (query.Start.Minute != 0 || query.Start.Second != 0 || query.Start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify minutes, seconds or milliseconds");
						if (query.End.Minute != 0 || query.End.Second != 0 || query.End.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify minutes, seconds or milliseconds");
						if (query.Start.Hour%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", query.Duration.Duration, query.Duration.Type));
						if (query.End.Hour%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", query.Duration.Duration, query.Duration.Type));
						break;
					case PeriodType.Days:
						if (query.Start.Hour != 0 || query.Start.Minute != 0 || query.Start.Second != 0 || query.Start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify hours, minutes, seconds or milliseconds");
						if (query.End.Hour != 0 || query.End.Minute != 0 || query.End.Second != 0 || query.End.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify hours, minutes, seconds or milliseconds");
						if (query.Start.Day%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", query.Duration.Duration, query.Duration.Type));
						if (query.End.Day%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", query.Duration.Duration, query.Duration.Type));
						break;
					case PeriodType.Months:
						if (query.Start.Day != 1 || query.Start.Hour != 0 || query.Start.Minute != 0 || query.Start.Second != 0 || query.Start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify days, hours, minutes, seconds or milliseconds");
						if (query.End.Day != 1 || query.End.Minute != 0 || query.End.Second != 0 || query.End.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by hours, you cannot specify days, hours, minutes, seconds or milliseconds");
						if (query.Start.Month%(query.Duration.Duration) != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", query.Duration.Duration, query.Duration.Type));
						if (query.End.Month%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", query.Duration.Duration, query.Duration.Type));
						break;
					case PeriodType.Years:
						if (query.Start.Month != 1 || query.Start.Day != 1 || query.Start.Hour != 0 || query.Start.Minute != 0 || query.Start.Second != 0 || query.Start.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by years, you cannot specify months, days, hours, minutes, seconds or milliseconds");
						if (query.End.Month != 1 || query.End.Day != 1 || query.End.Minute != 0 || query.End.Second != 0 || query.End.Millisecond != 0)
							throw new InvalidOperationException("When querying a roll up by years, you cannot specify months, days, hours, minutes, seconds or milliseconds");
						if (query.Start.Year%(query.Duration.Duration) != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that starts from midnight", query.Duration.Duration, query.Duration.Type));
						if (query.End.Year%query.Duration.Duration != 0)
							throw new InvalidOperationException(string.Format("Cannot create a roll up by {0} {1} as it cannot be divided to candles that ends in midnight", query.Duration.Duration, query.Duration.Type));
						break;
					default:
						throw new ArgumentOutOfRangeException();
				}

				if (string.IsNullOrWhiteSpace(query.Type))
					throw new InvalidOperationException("Type cannot be empty");

				var tree = tx.ReadTree(SeriesTreePrefix + query.Type);
				if (tree == null)
					yield break;

				var type = storage.GetType(query.Type);
				if (type == null)
					throw new InvalidOperationException("Type does not exist");

				using (var periodTx = storage.storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
				{
					var fixedTree = tree.FixedTreeFor(query.Key, (byte)(type.Fields.Length * sizeof(double)));
					var periodFixedTree = periodTx.State.GetTree(periodTx, PeriodTreePrefix + query.Type)
						.FixedTreeFor(query.Key + PeriodsKeySeparator + query.Duration.Type + "-" + query.Duration.Duration, (byte)(type.Fields.Length * Range.RangeValue.StorageItemsLength * sizeof(double)));

					using (var periodWriter = new RollupWriter(periodFixedTree, type.Fields.Length))
					{
						using (var periodTreeIterator = periodFixedTree.Iterate())
						using (var rawTreeIterator = fixedTree.Iterate())
						{
							foreach (var range in GetRanges(query, type.Fields.Length))
							{
								// seek period tree iterator, if found exact match!!, add and move to the next
								if (periodTreeIterator.Seek(range.StartAt.Ticks))
								{
									if (periodTreeIterator.CurrentKey == range.StartAt.Ticks)
									{
										var valueReader = periodTreeIterator.CreateReaderForCurrent();
										int used;
										var bytes = valueReader.ReadBytes(type.Fields.Length * Range.RangeValue.StorageItemsLength * sizeof(double), out used);
										Debug.Assert(used == type.Fields.Length*Range.RangeValue.StorageItemsLength*sizeof (double));

										for (int i = 0; i < type.Fields.Length; i++)
										{
											var startPosition = i * Range.RangeValue.StorageItemsLength;
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
								if (rawTreeIterator.Seek(range.StartAt.Ticks))
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

			private void GetAllPointsForRange(FixedSizeTree.IFixedSizeIterator rawTreeIterator, Range range, int valueLength)
			{
				var endTicks = range.Duration.AddToDateTime(range.StartAt).Ticks;
				var buffer = new byte[sizeof(double) * valueLength];
				var firstPoint = true;

				do
				{
					var ticks = rawTreeIterator.CurrentKey;
					if (ticks >= endTicks)
						return;

					var point = new Point
					{
#if DEBUG
						DebugKey = range.DebugKey,
#endif
						At = new DateTime(ticks),
						Values = new double[valueLength],
					};

					var reader = rawTreeIterator.CreateReaderForCurrent();
					reader.Read(buffer, 0, sizeof(double) * valueLength);

					for (int i = 0; i < valueLength; i++)
					{
						var value = point.Values[i] = EndianBitConverter.Big.ToDouble(buffer, i * sizeof(double));

						if (firstPoint)
						{
							range.Values[i].Open = range.Values[i].High = range.Values[i].Low = range.Values[i].Sum = value;
							range.Values[i].Volume = 1;
						}
						else
						{
							range.Values[i].High = Math.Max(range.Values[i].High, value);
							range.Values[i].Low = Math.Min(range.Values[i].Low, value);
							range.Values[i].Sum += value;
							range.Values[i].Volume += 1;
						}

						range.Values[i].Close = value;

					}
					firstPoint = false;

				} while (rawTreeIterator.MoveNext());
			}

			private IEnumerable<Range> GetRanges(TimeSeriesRollupQuery query, int valueLength)
			{
				var startAt = query.Start;
				while (true)
				{
					var nextStartAt = query.Duration.AddToDateTime(startAt);
					if (startAt == query.End)
						yield break;
					if (nextStartAt > query.End)
					{
						throw new InvalidOperationException("Debug: Duration is not aligned with the end of the range.");
					}
					var rangeValues = new Range.RangeValue[valueLength];
					for (int i = 0; i < valueLength; i++)
					{
						rangeValues[i] = new Range.RangeValue();
					}
					yield return new Range
					{
#if DEBUG
						DebugKey = query.Key,
#endif
						StartAt = startAt,
						Duration = query.Duration,
						Values = rangeValues,
					};
					startAt = nextStartAt;
				}
			}

			private IEnumerable<Point> GetRawQueryResult(TimeSeriesQuery query)
			{
				if (string.IsNullOrWhiteSpace(query.Type))
					throw new InvalidOperationException("Type cannot be empty");

				var tree = tx.ReadTree(SeriesTreePrefix + query.Type);
				if (tree == null)
					return Enumerable.Empty<Point>();

				var type = storage.GetType(query.Type);
				if (type == null)
					throw new InvalidOperationException("Type does not exist");
				var numberOfValues = type.Fields.Length;

				var buffer = new byte[numberOfValues * sizeof(double)];

				var fixedTree = tree.FixedTreeFor(query.Key, (byte) (numberOfValues*sizeof (double)));
				return IterateOnTree(query, fixedTree, it =>
				{
					var point = new Point
					{
#if DEBUG
						DebugKey = fixedTree.Name.ToString(),
#endif
						At = new DateTime(it.CurrentKey),
						Values = new double[numberOfValues],
					};
					
					var reader = it.CreateReaderForCurrent();
					reader.Read(buffer, 0, numberOfValues * sizeof (double));
					for (int i = 0; i < numberOfValues; i++)
					{
						point.Values[i] = EndianBitConverter.Big.ToDouble(buffer, i * sizeof(double));
					}
					return point;
				});
			}

			public static IEnumerable<T> IterateOnTree<T>(TimeSeriesQuery query, FixedSizeTree fixedTree, Func<FixedSizeTree.IFixedSizeIterator, T> iteratorFunc)
			{
				using (var it = fixedTree.Iterate())
				{
					if (it.Seek(query.Start.Ticks) == false)
						yield break;

					do
					{
						if (it.CurrentKey > query.End.Ticks)
							yield break;

						yield return iteratorFunc(it);
					} while (it.MoveNext());
				}
			}

			public void Dispose()
			{
				if (tx != null)
					tx.Dispose();
			}

			public IEnumerable<TimeSeriesPoint> GetPoints(string typeName, string key, int skip)
			{
				var tree = tx.ReadTree(SeriesTreePrefix + typeName);
				if (tree == null)
					yield break;

				var type = storage.GetType(typeName);
				if (type == null)
					throw new InvalidOperationException("Type does not exist");
				var buffer = new byte[type.Fields.Length * sizeof(double)];

				var fixedTree = tree.FixedTreeFor(key, (byte) (type.Fields.Length*sizeof (double)));
				using (var fixedIt = fixedTree.Iterate())
				{
					if (fixedIt.Seek(DateTime.MinValue.Ticks) && (skip == 0 || fixedIt.Skip(skip)))
					{
						do
						{
							var point = new TimeSeriesPoint
							{
								At = new DateTime(fixedIt.CurrentKey),
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
			}

			public TimeSeriesKey GetKey(string typeName, string key)
			{
				var type = storage.GetType(typeName);
				if (type == null)
					return null;

				var tree = tx.ReadTree(SeriesTreePrefix + typeName);
				var fixedTree = tree.FixedTreeFor(key, (byte) (type.Fields.Length*sizeof (double)));
				return new TimeSeriesKey
				{
					Type = type,
					Key = key,
					PointsCount = fixedTree.NumberOfEntries,
				};
			}

			public IEnumerable<TimeSeriesKey> GetKeys(string typeName, int skip)
			{
				var type = storage.GetType(typeName);
				if (type == null)
					yield break;

				var tree = tx.ReadTree(SeriesTreePrefix + typeName);
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
				using (var rootIt = tx.State.Root.Iterate())
				{
					rootIt.RequiredPrefix = SeriesTreePrefix;
					if (rootIt.Seek(rootIt.RequiredPrefix) && (skip == 0 || rootIt.Skip(skip)))
					{
						do
						{
							var typeTreeName = rootIt.CurrentKey.ToString();
							var type = storage.GetType(typeTreeName.Replace(SeriesTreePrefix, ""));
							var tree = tx.ReadTree(typeTreeName);
							type.KeysCount = tree.State.EntriesCount;
							yield return type;
						} while (rootIt.MoveNext());
					}
				}
			}
		}

		public class Writer : IDisposable
		{
			private readonly TimeSeriesStorage storage;
			private readonly Transaction tx;

			private Tree tree;
			private readonly Dictionary<string, RollupRange> rollupsToClear = new Dictionary<string, RollupRange>();
			private TimeSeriesType currentType;
			private readonly Dictionary<byte, byte[]> valBuffers = new Dictionary<byte, byte[]>(); 

			public Writer(TimeSeriesStorage storage)
			{
				this.storage = storage;
				currentType = new TimeSeriesType();
				tx = storage.storageEnvironment.NewTransaction(TransactionFlags.ReadWrite);
			}

			public bool Append(string type, string key, DateTime time, params double[] values)
			{
				if (string.IsNullOrWhiteSpace(type))
					throw new ArgumentOutOfRangeException("type", "Type cannot be empty");
				if (string.IsNullOrWhiteSpace(key))
					throw new ArgumentOutOfRangeException("key", "Key cannot be empty");

				if (currentType.Type != type)
				{
					currentType = storage.GetType(type);
					if (currentType == null)
						throw new InvalidOperationException("There is no type named: " + type);

					tree = tx.State.GetTree(tx, SeriesTreePrefix + currentType.Type);
				}

				if (values.Length != currentType.Fields.Length)
					throw new ArgumentOutOfRangeException("values", string.Format("Appended values should be the same length the series values length which is {0} and not {1}", currentType.Fields.Length, values.Length));

				RollupRange range;
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
					rollupsToClear.Add(clearKey, new RollupRange(type, key, time));
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
					storage.UpdateKeysCount(tx, 1);
				}
				var newPointWasAppended = fixedTree.Add(time.Ticks, valBuffer);
				if (newPointWasAppended)
				{
					storage.UpdatePointsCount(tx, 1);
				}
				return newPointWasAppended;
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
					DeleteRangeInRollups(rollupRange.Type, rollupRange.Key, rollupRange.Start.Ticks, rollupRange.End.Ticks);
				}
				tx.Commit();
			}

			public void DeleteRangeInRollups(string typeName, string key, long start, long end)
			{
				var periodTree = tx.ReadTree(PeriodTreePrefix + typeName);
				if (periodTree == null)
					return;

				using (var it = periodTree.Iterate())
				{
					it.RequiredPrefix = key + PeriodsKeySeparator;
					if (it.Seek(it.RequiredPrefix) == false)
						return;

					var type = storage.GetType(typeName);
					if (type == null)
						throw new InvalidOperationException("There is no type named: " + typeName);

					do
					{
						
						var periodTreeName = it.CurrentKey.ToString();
						var periodFixedTree = periodTree.FixedTreeFor(periodTreeName, (byte) (type.Fields.Length*Range.RangeValue.StorageItemsLength*sizeof (double)));
						if (periodFixedTree == null)
							continue;

						var duration = GetDurationFromTreeName(periodTreeName);
						var keysToDelete = Reader.IterateOnTree(new TimeSeriesQuery
						{
							Type = typeName,
							Key = key,
							Start = duration.GetStartOfRangeForDateTime(new DateTime(start)),
							End = duration.GetStartOfRangeForDateTime(new DateTime(end)),
						}, periodFixedTree, fixedIterator => fixedIterator.CurrentKey).ToArray();

						foreach (var keyToDelete in keysToDelete)
						{
							periodFixedTree.Delete(keyToDelete);
						}
					} while (it.MoveNext());
				}
			}

			private PeriodDuration GetDurationFromTreeName(string periodTreeName)
			{
				var separatorIndex = periodTreeName.LastIndexOf(PeriodsKeySeparator);
				var s = periodTreeName.Substring(separatorIndex + 1);
				var strings = s.Split('-');
				return new PeriodDuration(GenericUtil.ParseEnum<PeriodType>(strings[0]), int.Parse(strings[1]));
			}

			public bool Delete(string typeName, string key)
			{
				tree = tx.ReadTree(SeriesTreePrefix + typeName);
				if (tree == null)
					return false;

				var type = storage.GetType(typeName);
				if (type == null)
					throw new InvalidOperationException("There is no type named: " + typeName);

				var numberOfEntriesRemoved = tree.DeleteFixedTreeFor(key, (byte)(type.Fields.Length * sizeof(double)));
				storage.UpdatePointsCount(tx, -numberOfEntriesRemoved);
				storage.UpdateKeysCount(tx, -1);
				
				return true;
			}

			public bool DeletePoint(TimeSeriesPointId point)
			{
				tree = tx.ReadTree(SeriesTreePrefix + point.Type);
				if (tree == null)
					return false;

				var type = storage.GetType(point.Type);
				if (type == null)
					throw new InvalidOperationException("There is no type named: " + point.Type);

				var fixedTree = tree.FixedTreeFor(point.Key, (byte)(type.Fields.Length * sizeof(double)));
				var result = fixedTree.Delete(point.At.Ticks);
				if (result.NumberOfEntriesDeleted > 0)
				{
					storage.UpdatePointsCount(tx, -1);
				}
				if (result.TreeRemoved)
				{
					storage.UpdateKeysCount(tx, -1);
				}

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
				tree = tx.ReadTree(SeriesTreePrefix + typeName);
				if (tree == null)
					return;

				var type = storage.GetType(typeName);
				if (type == null)
					throw new InvalidOperationException("There is no type named: " + typeName);

				var fixedTree = tree.FixedTreeFor(key, (byte)(type.Fields.Length * sizeof(double)));
				var result = fixedTree.DeleteRange(start, end);
				storage.UpdatePointsCount(tx, -result.NumberOfEntriesDeleted);
				if (result.TreeRemoved)
					storage.UpdateKeysCount(tx, -1);
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
				valBuffer = new byte[numberOfValues * Range.RangeValue.StorageItemsLength * sizeof(double)];
			}

			public void Append(DateTime time, Range range)
			{
				for (int i = 0; i < numberOfValues; i++)
				{
					var rangeValue = range.Values[i];
					var startPosition = i * Range.RangeValue.StorageItemsLength;
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

				tree.Add(time.Ticks, valBuffer);
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

		public TimeSeriesStats CreateStats()
		{
			var stats = new TimeSeriesStats
			{
				Name = Name,
				Url = TimeSeriesUrl,
				TypesCount = GetTypesCount(),
				KeysCount = GetKeysCount(),
				PointsCount = GetPointsCount(),
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

		public void CreateType(TimeSeriesType type)
		{
			if (type == null || string.IsNullOrWhiteSpace(type.Type))
				throw new InvalidOperationException("Type cannot be empty");

			if (type.Fields.Length < 1)
				throw new InvalidOperationException("Fields length should be equal or greater than 1");

			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var metadata = tx.ReadTree("$metadata");
				var val = metadata.Read(TypesPrefix + type.Type);
				if (val != null)
				{
					throw new InvalidOperationException(string.Format("Type '{0}' is already created", type.Type));
				}

				using (var ms = new MemoryStream())
				using (var writer = new StreamWriter(ms))
				using (var jsonTextWriter = new JsonTextWriter(writer))
				{
					JsonSerializer.Serialize(jsonTextWriter, type);
					writer.Flush();
					ms.Position = 0;
					metadata.Add(TypesPrefix + type.Type, ms);
					UpdateTypesCount(tx, 1);
				}
				tx.Commit();
			}
		}

		public void DeleteType(string type)
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var metadata = tx.ReadTree("$metadata");
				var val = metadata.Read(TypesPrefix + type);
				if (val == null)
					throw new InvalidOperationException(string.Format("Type {0} does not exist", type));
				
				AssertNoExistDataForType(type, tx);
				
				metadata.Delete(TypesPrefix + type);
				UpdateTypesCount(tx, -1);
				tx.Commit();
			}
		}

		private void AssertNoExistDataForType(string type, Transaction tx)
		{
			var tree = tx.ReadTree(SeriesTreePrefix + type);
			if (tree == null)
				return;

			using (var it = tree.Iterate())
			{
				if (it.Seek(Slice.BeforeAllKeys) == false)
					return;
			}

			throw new InvalidOperationException(string.Format("Cannot delete type '{0}' since there is associated data to it", type));
		}

		public TimeSeriesType GetType(string type)
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var metadata = tx.ReadTree("$metadata");
				var readResult = metadata.Read(TypesPrefix + type);
				if (readResult == null)
					return null;

				using (var stream = readResult.Reader.AsStream())
				{
					stream.Position = 0;
					using (var streamReader = new StreamReader(stream))
					using (var jsonTextReader = new JsonTextReader(streamReader))
					{
						return new JsonSerializer().Deserialize<TimeSeriesType>(jsonTextReader);
					}
				}
			}
		}

		private long GetTypesCount()
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.Read))
			{
				var metadata = tx.ReadTree("$metadata");
				var val = metadata.Read(StatsPrefix + "types-count");
				if (val == null)
					return CalculateTypesCount();
				var count = val.Reader.ReadLittleEndianInt64();
				return count;
			}
		}

		public long GetKeysCount()
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.Read))
			{
				var metadata = tx.ReadTree("$metadata");
				var val = metadata.Read(StatsPrefix + "keys-count");
				if (val == null)
					return 0;
				var count = val.Reader.ReadLittleEndianInt64();
				return count;
			}
		}

		public long GetPointsCount()
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.Read))
			{
				var metadata = tx.ReadTree("$metadata");
				var val = metadata.Read(StatsPrefix + "points-count");
				if (val == null)
					return 0;
				var count = val.Reader.ReadLittleEndianInt64();
				return count;
			}
		}

		public void CalculateKeysAndPointsCount()
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				long keys = 0, points = 0;
				using (var rootIt = tx.State.Root.Iterate())
				{
					rootIt.RequiredPrefix = SeriesTreePrefix;
					if (rootIt.Seek(rootIt.RequiredPrefix))
					{
						do
						{
							var typeTreeName = rootIt.CurrentKey.ToString();
							var type = GetType(typeTreeName.Replace(SeriesTreePrefix, ""));
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

		private void UpdateKeysCount(Transaction tx, long delta)
		{
			var metadata = tx.ReadTree("$metadata");
			metadata.Increment(StatsPrefix + "keys-count", delta);
		}

		private void UpdatePointsCount(Transaction tx, long delta)
		{
			var metadata = tx.ReadTree("$metadata");
			metadata.Increment(StatsPrefix + "points-count", delta);
		}

		private void UpdateTypesCount(Transaction tx, long delta)
		{
			var metadata = tx.ReadTree("$metadata");
			metadata.Increment(StatsPrefix + "types-count", delta);
		}

		/// <summary>
		/// This intended to be here for testing and debugging. This code is not reachable in production, since we always cache the stats.
		/// </summary>
		private long CalculateTypesCount()
		{
			long count = 0;

			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var metadata = tx.ReadTree("$metadata");
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

				var val = new byte[sizeof (long)];
				EndianBitConverter.Little.CopyBytes(count, val, 0);
				metadata.Add(new Slice(StatsPrefix + "types-count"), new Slice(val));
			}

			return count;
		}
	}
}