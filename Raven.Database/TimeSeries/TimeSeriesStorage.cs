using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Abstractions;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
using Raven.Database.Util;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Trees.Fixed;
using Voron.Util.Conversion;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Database.TimeSeries
{
	public class TimeSeriesStorage : IResourceStore, IDisposable
	{
		private readonly StorageEnvironment storageEnvironment;
		private readonly TransportState transportState;

		public Guid ServerId { get; set; }

		public string TimeSeriesUrl { get; private set; }

		public string Name { get; private set; }
		public string ResourceName { get; private set; }
		public TransportState TransportState { get; private set; }
		public AtomicDictionary<object> ExtensionsState { get; private set; }
		public InMemoryRavenConfiguration Configuration { get; private set; }
		public DateTime LastWrite { get; set; }

		public TimeSeriesStorage(string serverUrl, string timeSeriesName, InMemoryRavenConfiguration configuration, TransportState receivedTransportState = null)
		{
			Name = timeSeriesName;
			TimeSeriesUrl = string.Format("{0}ts/{1}", serverUrl, timeSeriesName);
			ResourceName = string.Concat(Constants.TimeSeries.UrlPrefix, "/", timeSeriesName);

			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.TimeSeries.DataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);
			transportState = receivedTransportState ?? new TransportState();
			ExtensionsState = new AtomicDictionary<object>();

			Configuration = configuration;
			Initialize();
		}

		private void Initialize()
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				storageEnvironment.CreateTree(tx, "data", keysPrefixing: true);

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
			return CreateReader(SeriesType.Simple());
		}

		public Reader CreateReader(SeriesType seriesType)
		{
			return new Reader(this, seriesType);
		}

		public Writer CreateWriter(SeriesType seriesType)
		{
			LastWrite = SystemTime.UtcNow;
			return new Writer(this, seriesType);
		}

		public class Point
		{
#if DEBUG
			public string DebugKey { get; set; }
#endif
			public DateTime At { get; set; }

			public double Value { get; set; }
		}

		public class Reader : IDisposable
		{
			private readonly TimeSeriesStorage storage;
			private readonly SeriesType type;
			private readonly Transaction tx;
			private readonly Tree tree;

			public Reader(TimeSeriesStorage storage, SeriesType type)
			{
				this.storage = storage;
				this.type = type;
				tx = this.storage.storageEnvironment.NewTransaction(TransactionFlags.Read);
				tree = tx.State.GetTree(tx, type.Type);
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
				throw new NotImplementedException();
				// return GetQueryRollup(query);
			}

			public IEnumerable<Range>[] QueryRollup(params TimeSeriesRollupQuery[] queries)
			{
				var result = new IEnumerable<Range>[queries.Length];
				for (int i = 0; i < queries.Length; i++)
				{
					throw new NotImplementedException();
					//result[i] = GetQueryRollup(queries[i]);
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


				using (var periodTx = storage.storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
				{
					var periodFixedTree = periodTx.State.GetTree(periodTx, "period_type_" + type.Name)
						.FixedTreeFor(query.Duration.Type + "-" + query.Duration.Duration + "-" + query.Key, type.Size);
					using (var writer = new RollupWriter(periodFixedTree))
					{
						using (var periodTreeIterator = periodFixedTree.Iterate())
						using (var rawTreeIterator = tfree.Iterate())
						{
							var keyBytesLen = Encoding.UTF8.GetByteCount(query.Key) + sizeof (long);
							var startKeyWriter = new SliceWriter(keyBytesLen);
							startKeyWriter.Write(query.Key);
							var prefixKey = startKeyWriter.CreateSlice();

							periodTreeIterator.RequiredPrefix = prefixKey;
							rawTreeIterator.RequiredPrefix = prefixKey;

							foreach (var range in GetRanges(query))
							{
								var seekWriter = new SliceWriter(keyBytesLen);
								seekWriter.Write(query.Key);
								seekWriter.Write(range.StartAt.Ticks);
								var seekSlice = seekWriter.CreateSlice();

								// seek period tree iterator, if found exact match!!, add and move to the next
								if (periodTreeIterator.Seek(seekSlice)
								    && periodTreeIterator.CurrentKey.KeyLength == keyBytesLen)
								{
									var keyReader = periodTreeIterator.CurrentKey.CreateReader();
									keyReader.Skip(keyBytesLen - sizeof (long));
									var ticks = keyReader.ReadBigEndianInt64();
									if (ticks == range.StartAt.Ticks)
									{
										var structureReader = periodTreeIterator.ReadStructForCurrent(RollupWriter.RangeSchema);
										range.Volume = structureReader.ReadInt(PointCandleSchema.Volume);
										if (range.Volume != 0)
										{
											range.High = structureReader.ReadDouble(PointCandleSchema.High);
											range.Low = structureReader.ReadDouble(PointCandleSchema.Low);
											range.Open = structureReader.ReadDouble(PointCandleSchema.Open);
											range.Close = structureReader.ReadDouble(PointCandleSchema.Close);
											range.Sum = structureReader.ReadDouble(PointCandleSchema.Sum);
										}
										yield return range;
										continue;
									}
								}

								// seek tree iterator, if found note don't go to next range!!, sum, add to period tree, move next
								if (range.StartAt.Minute == 0 && range.StartAt.Second == 0)
								{
									
								}
								if (rawTreeIterator.Seek(seekSlice))
								{
									GetAllPointsForRange(rawTreeIterator, keyBytesLen, range);
								}

								// if not found, create empty periods until the end or the next valid period
								/*if (range.Volume == 0)
								{
								}*/

								writer.Append(query.Key, range.StartAt, range);
								yield return range;
							}
						}
					}
					periodTx.Commit();
				}
			}

			private void GetAllPointsForRange(TreeIterator rawTreeIterator, int keyBytesLen, Range range)
			{
				var endTicks = range.Duration.AddToDateTime(range.StartAt).Ticks;
				var buffer = new byte[sizeof(double)];
				Point firstPoint = null;

				do
				{
					if (rawTreeIterator.CurrentKey.KeyLength != keyBytesLen) // avoid getting another key (A1, A10, etc)
						return;

					var keyReader = rawTreeIterator.CurrentKey.CreateReader();
					keyReader.Skip(keyBytesLen - sizeof(long));
					var ticks = keyReader.ReadBigEndianInt64();
					if (ticks >= endTicks)
						return;

					var point = new Point
					{
#if DEBUG
						DebugKey = keyReader.AsPartialSlice(sizeof(long)).ToString(),
#endif
						At = new DateTime(ticks),
					};

					var reader = rawTreeIterator.CreateReaderForCurrent();
					reader.Read(buffer, 0, sizeof(double));
					point.Value = EndianBitConverter.Big.ToDouble(buffer, 0);

					if (firstPoint == null)
					{
						firstPoint = point;
						range.Open = point.Value;
						range.High = point.Value;
						range.Low = point.Value;
						range.Sum = point.Value;
						range.Volume = 1;
					}
					else
					{
						range.High = Math.Max(range.High, point.Value);
						range.Low = Math.Min(range.Low, point.Value);
						range.Sum += point.Value;
						range.Volume += 1;
					}

					range.Close = point.Value;
				} while (rawTreeIterator.MoveNext());
			}

			private IEnumerable<Range> GetRanges(TimeSeriesRollupQuery query)
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
					yield return new Range
					{
#if DEBUG
						DebugKey = query.Key,
#endif
						StartAt = startAt,
						Duration = query.Duration,
					};
					startAt = nextStartAt;
				}
			}

			private IEnumerable<Point> GetRawQueryResult(TimeSeriesQuery query)
			{
				var buffer = new byte[sizeof(double)];

				var fixedTree = tree.FixedTreeFor(query.Key, type.Size);
				return IterateOnTree(query, fixedTree, it =>
				{
					var point = new Point
					{
#if DEBUG
						DebugKey = fixedTree.Name.ToString(),
#endif
						At = new DateTime(it.CurrentKey),
					};
					
					var reader = it.CreateReaderForCurrent();
					reader.Read(buffer, 0, sizeof (double));
					point.Value = EndianBitConverter.Big.ToDouble(buffer, 0);
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

			public long GetTimeSeriesCount()
			{
				throw new InvalidOperationException();
			}
		}

		public class Writer : IDisposable
		{
			private readonly SeriesType type;
			private readonly Transaction tx;

			private readonly Tree tree;
			private readonly Dictionary<string, RollupRange> rollupsToClear = new Dictionary<string, RollupRange>();

			public Writer(TimeSeriesStorage storage, SeriesType type)
			{
				this.type = type;
				tx = storage.storageEnvironment.NewTransaction(TransactionFlags.ReadWrite); 
				tree = tx.State.GetTree(tx, type.Type);
			}

			public void Append(string key, DateTime time, object value)
			{
				RollupRange range;
				if (rollupsToClear.TryGetValue(key, out range))
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
					rollupsToClear.Add(key, new RollupRange(time));
				}
				
				var fixedTree = tree.FixedTreeFor(key, type.Size);
				fixedTree.Add(time.Ticks, type.ParseValue(value));
			}

			public void Dispose()
			{
				
				if (tx != null)
					tx.Dispose();
			}

			public void Commit()
			{
				// CleanRollupDataForRange();
				tx.Commit();
			}

			/*private void CleanRollupDataForRange()
			{
				using (var it = tx.State.Root.Iterate())
				{
					it.RequiredPrefix = "period_";
					if (it.Seek(it.RequiredPrefix) == false)
						return;

					do
					{
						var treeName = it.CurrentKey.ToString();
						var periodTree = tx.ReadTree(treeName);
						if (periodTree == null)
							continue;

						CleanDataFromPeriodTree(periodTree, PeriodDuration.ParseTreeName(treeName));
					} while (it.MoveNext());
				}
			}

			private void CleanDataFromPeriodTree(Tree periodTree, PeriodDuration duration)
			{
				foreach (var rollupRange in rollupsToClear)
				{
					var keysToDelete = Reader.IterateOnTree(new TimeSeriesQuery
					{
						Key = rollupRange.Key,
						Start = duration.GetStartOfRangeForDateTime(rollupRange.Value.Start),
						End = duration.GetStartOfRangeForDateTime(rollupRange.Value.End),
					}, periodTree, (iterator, keyReader, ticks) => iterator.CurrentKey).ToArray();

					foreach (var key in keysToDelete)
					{
						periodTree.Delete(key);
					}
				}
			}*/
		}

		public class RollupWriter : IDisposable
		{
			public static readonly StructureSchema<PointCandleSchema> RangeSchema;

			static RollupWriter()
			{
				RangeSchema = new StructureSchema<PointCandleSchema>()
					.Add<int>(PointCandleSchema.Volume)
					.Add<double>(PointCandleSchema.High)
					.Add<double>(PointCandleSchema.Low)
					.Add<double>(PointCandleSchema.Open)
					.Add<double>(PointCandleSchema.Close)
					.Add<double>(PointCandleSchema.Sum);
			}

			private readonly FixedSizeTree _tree;

			private readonly byte[] _keyBuffer = new byte[1024];

			public RollupWriter(FixedSizeTree tree)
			{
				_tree = tree;
			}

			public void Append(string key, DateTime time, Range range)
			{
				var sliceWriter = new SliceWriter(_keyBuffer);
				sliceWriter.Write(key);
				sliceWriter.Write(time.Ticks);
				var keySlice = sliceWriter.CreateSlice();

				var structure = new Structure<PointCandleSchema>(RangeSchema);

				structure.Set(PointCandleSchema.Volume, range.Volume);
				if (range.Volume != 0)
				{
					structure.Set(PointCandleSchema.High, range.High);
					structure.Set(PointCandleSchema.Low, range.Low);
					structure.Set(PointCandleSchema.Open, range.Open);
					structure.Set(PointCandleSchema.Close, range.Close);
					structure.Set(PointCandleSchema.Sum, range.Sum);
				}

				var ptr = _tree.DirectAdd(keySlice);
				structure.Write(ptr);
			}

			public void Dispose()
			{
			}
		}

		public void Dispose()
		{
			if (storageEnvironment != null)
				storageEnvironment.Dispose();
		}

		public TimeSeriesStats CreateStats()
		{
			using (var reader = CreateReader())
			{
				var stats = new TimeSeriesStats
				{
					Name = Name,
					Url = TimeSeriesUrl,
					TimeSeriesCount = reader.GetTimeSeriesCount(),
					TimeSeriesSize = SizeHelper.Humane(TimeSeriesEnvironment.Stats().UsedDataFileSizeInBytes),
					// RequestsPerSecond = Math.Round(metricsTimeSeries.RequestsPerSecondTimeSeries.CurrentValue, 3),
				};
				return stats;
			}
		}

		public StorageEnvironment TimeSeriesEnvironment
		{
			get { return storageEnvironment; }
		}
	}
}