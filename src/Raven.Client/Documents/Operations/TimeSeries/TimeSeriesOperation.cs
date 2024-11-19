using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    /// <summary>
    /// Represents a batch operation for time series data, including appends, increments, and deletions, 
    /// to be performed on a single document’s time series.
    /// </summary>
    public sealed class TimeSeriesOperation
    {
        private SortedList<long, AppendOperation> _appends;
        private SortedList<long, IncrementOperation> _increments;

        internal IList<AppendOperation> Appends
        {
            get => _appends?.Values;
            private set
            {
                _appends ??= new SortedList<long, AppendOperation>();
                foreach (var appendOperation in value)
                {
                    _appends[appendOperation.Timestamp.Ticks] = appendOperation;
                }
            }
        }

        internal IList<IncrementOperation> Increments
        {
            get => _increments?.Values;
            private set
            {
                if (value == null)
                    return;
                foreach (var incrementOperation in value)
                {
                    Increment(incrementOperation);
                }
            }
        }

        /// <summary>
        /// Adds an incremental operation to the batch for the specified time series.
        /// This operation increments the values at the given timestamp, supporting concurrent updates in a distributed environment.
        /// </summary>
        /// <param name="incrementOperation">The increment operation to add, specifying the timestamp and values to increment.</param>
        /// <remarks>
        /// Incremental time series in RavenDB are designed to handle concurrent updates from multiple nodes.
        /// Each node maintains its own local changes for the specified timestamp, which are aggregated to provide a unified view.
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the number of values in the new operation does not match the number in an existing operation for the same timestamp.
        /// </exception>
        public void Increment(IncrementOperation incrementOperation)
        {
            _increments ??= new SortedList<long, IncrementOperation>();
            if (_increments.TryGetValue(incrementOperation.Timestamp.Ticks, out var existing))
            {
                if (existing.Values.Length != incrementOperation.Values.Length)
                    throw new InvalidOperationException(
                        $"Previous increment to timestamp {incrementOperation.Timestamp} had different number of values {existing.Values.Length} vs. current with {incrementOperation.Values.Length}");

                for (int i = 0; i < existing.Values.Length; i++)
                {
                    existing.Values[i] += incrementOperation.Values[i];
                }
            }
            else
            {
                _increments[incrementOperation.Timestamp.Ticks] = incrementOperation;
            }
        }

        internal List<DeleteOperation> Deletes;

        /// <summary>
        /// Gets or sets the name of the time series on which the operations are performed.
        /// This property is mandatory and must be set before executing the <see cref="TimeSeriesBatchOperation"/> that contains this <see cref="TimeSeriesOperation"/>.
        /// </summary>
        /// <remarks>
        /// The <see cref="Name"/> property identifies the time series within the document to which the batch operations apply.
        /// If this property is not set, an exception will be thrown when attempting to execute the batch operation.
        /// </remarks>
        public string Name { get; set; }

        /// <summary>
        /// Adds an append operation to the batch.
        /// This operation adds new data points to the time series at a specific timestamp.
        /// </summary>
        /// <param name="appendOperation">The append operation to add.</param>
        public void Append(AppendOperation appendOperation)
        {
            _appends ??= new SortedList<long, AppendOperation>();
            appendOperation.Timestamp = appendOperation.Timestamp.EnsureUtc().EnsureMilliseconds();
            _appends[appendOperation.Timestamp.Ticks] = appendOperation; // on duplicate values the last one overrides
        }

        /// <summary>
        /// Adds a delete operation to the batch.
        /// This operation removes data points within a specific range from the time series.
        /// </summary>
        /// <param name="deleteOperation">The delete operation to add.</param>
        public void Delete(DeleteOperation deleteOperation)
        {
            Deletes ??= new List<DeleteOperation>();
            deleteOperation.To = deleteOperation.To?.EnsureUtc();
            deleteOperation.From = deleteOperation.From?.EnsureUtc();
            Deletes.Add(deleteOperation);
        }

        internal static TimeSeriesOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet(nameof(Name), out string name) == false || name == null)
                ThrowMissingProperty<TimeSeriesOperation>(nameof(Name));

            var result = new TimeSeriesOperation
            {
                Name = name
            };

            if (input.TryGet(nameof(Appends), out BlittableJsonReaderArray operations) && operations != null)
            {
                var sorted = new SortedList<long, AppendOperation>();
                foreach (var op in operations)
                {
                    if (!(op is BlittableJsonReaderObject bjro))
                    {
                        ThrowNotBlittableJsonReaderObjectOperation(op);
                        return null; //never hit
                    }

                    var append = AppendOperation.Parse(bjro);

                    sorted[append.Timestamp.Ticks] = append;
                }
                result._appends = sorted;
            }

            if (input.TryGet(nameof(Deletes), out operations) && operations != null)
            {
                result.Deletes = new List<DeleteOperation>();

                foreach (var op in operations)
                {
                    if (!(op is BlittableJsonReaderObject bjro))
                    {
                        ThrowNotBlittableJsonReaderObjectOperation(op);
                        return null; //never hit
                    }

                    result.Deletes.Add(DeleteOperation.Parse(bjro));
                }
            }

            if (input.TryGet(nameof(Increments), out operations) && operations != null)
            {

                foreach (var op in operations)
                {
                    if (!(op is BlittableJsonReaderObject bjro))
                    {
                        ThrowNotBlittableJsonReaderObjectOperation(op);
                        return null; //never hit
                    }

                    result.Increment(IncrementOperation.Parse(bjro));
                }
            }

            return result;
        }

        private const string TimeSeriesFormat = "TimeFormat";
        private static readonly long EpochTicks = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        internal enum TimeFormat
        {
            DotNetTicks,
            UnixTimeInMs,
            UnixTimeInNs
        }

        private static long FromUnixMs(long unixMs)
        {
            return unixMs * 10_000 + EpochTicks;
        }

        private static long FromUnixNs(long unixNs)
        {
            return unixNs / 100 + EpochTicks;
        }

        internal static TimeSeriesOperation ParseForBulkInsert(BlittableJsonReaderObject input)
        {
            if (input.TryGet(nameof(Name), out string name) == false || name == null)
                ThrowMissingProperty<TimeSeriesOperation>(nameof(Name));

            input.TryGet(TimeSeriesFormat, out TimeFormat format);

            var result = new TimeSeriesOperation
            {
                Name = name
            };

            if (input.TryGet(nameof(Appends), out BlittableJsonReaderArray operations) == false || operations == null)
                ThrowMissingProperty<TimeSeriesOperation>(nameof(Appends));

            var sorted = new SortedList<long, AppendOperation>();
            foreach (var op in operations)
            {
                if (op is not BlittableJsonReaderArray bjro)
                {
                    ThrowNotBlittableJsonReaderArrayOperation(op);
                    return null; //never hit
                }

                var time = GetLong(bjro[0]);

                switch (format)
                {
                    case TimeFormat.UnixTimeInMs:
                        time = FromUnixMs(time);
                        break;
                    case TimeFormat.UnixTimeInNs:
                        time = FromUnixNs(time);
                        break;
                    case TimeFormat.DotNetTicks:
                        break;
                    default:
                        throw new ArgumentException($"Unknown time-format '{format}'");
                }

                var append = new AppendOperation
                {
                    Timestamp = new DateTime(time)
                };

                var numberOfValues = GetLong(bjro[1]);
                var doubleValues = new double[numberOfValues];

                for (var i = 0; i < numberOfValues; i++)
                {
                    var obj = bjro[i + 2];
                    switch (obj)
                    {
                        case long l:
                            // when we send the number without the decimal point
                            // this is the same as what Convert.ToDouble is doing
                            doubleValues[i] = l;
                            break;

                        case LazyNumberValue lnv:
                            doubleValues[i] = lnv;
                            break;

                        default:
                            doubleValues[i] = Convert.ToDouble(obj);
                            break;
                    }
                }

                append.Values = doubleValues;

                var tagIndex = 2 + numberOfValues;
                if (bjro.Length > tagIndex)
                {
                    if (BlittableJsonReaderObject.ChangeTypeToString(bjro[(int)tagIndex], out string tagAsString) == false)
                        ThrowNotString(bjro[0]);

                    append.Tag = tagAsString;
                }

                sorted[time] = append;
            }

            result._appends = sorted;

            return result;

            static long GetLong(object value)
            {
                return value switch
                {
                    long l => l,
                    LazyNumberValue lnv => lnv,
                    _ => throw new NotSupportedException($"Not supported type. Was expecting number, but got '{value}'."),
                };
            }
        }

        private static void ThrowNotBlittableJsonReaderObjectOperation(object op)
        {
            throw new InvalidDataException($"'Operations' should contain items of type BlittableJsonReaderObject only, but got {op.GetType()}");
        }

        private static void ThrowNotBlittableJsonReaderArrayOperation(object op)
        {
            throw new InvalidDataException($"'Appends' should contain items of type BlittableJsonReaderArray only, but got {op.GetType()}");
        }

        private static void ThrowNotString(object obj)
        {
            throw new InvalidDataException($"Expected a string but got: {obj.GetType()}");
        }

        private static void ThrowMissingProperty<T>(string prop)
        {
            throw new InvalidDataException($"Missing '{prop}' property on TimeSeriesOperation '{typeof(T).Name}'");
        }

        /// <summary>
        /// Converts the time series operation into a JSON representation.
        /// </summary>
        /// <returns>A <see cref="DynamicJsonValue"/> representing the operation.</returns>
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Appends)] = Appends?.Select(x => x.ToJson()),
                [nameof(Deletes)] = Deletes?.Select(x => x.ToJson()),
                [nameof(Increments)] = Increments?.Select(x => x.ToJson())
            };
        }

        /// <summary>
        /// Represents an append operation in a time series, allowing new data points to be added at specific timestamps.
        /// </summary>
        public sealed class AppendOperation
        {
            /// <summary>
            /// Gets or sets the timestamp of the data point to be appended.
            /// This specifies when the data point occurred.
            /// </summary>
            public DateTime Timestamp { get; set; }

            /// <summary>
            /// Gets or sets the values associated with the data point.
            /// These are the numeric measurements recorded at the specified <see cref="Timestamp"/>.
            /// </summary>
            public double[] Values { get; set; }

            /// <summary>
            /// Gets or sets an optional tag for the data point.
            /// The tag can provide additional context or metadata for the appended data, such as the source or a descriptive label.
            /// </summary>
            public string Tag { get; set; }
            internal static AppendOperation Parse(BlittableJsonReaderObject input)
            {
                if (input.TryGet(nameof(Timestamp), out DateTime ts) == false)
                    throw new InvalidDataException($"Missing '{nameof(Timestamp)}' property");

                if (input.TryGet(nameof(Values), out BlittableJsonReaderArray values) == false || values == null)
                    throw new InvalidDataException($"Missing '{nameof(Values)}' property");

                input.TryGet(nameof(Tag), out string tag); // optional

                var doubleValues = new double[values.Length];
                for (int i = 0; i < doubleValues.Length; i++)
                {
                    doubleValues[i] = values.GetByIndex<double>(i);
                }

                var op = new AppendOperation
                {
                    Timestamp = ts,
                    Values = doubleValues,
                    Tag = tag
                };

                return op;
            }

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(Timestamp)] = Timestamp,
                    [nameof(Values)] = new DynamicJsonArray(Values),
                };

                if (Tag != null)
                    djv[nameof(Tag)] = Tag;

                return djv;
            }
        }

        /// <summary>
        /// Represents a delete operation in a time series, allowing data points within a specific range to be removed.
        /// </summary>
        public sealed class DeleteOperation
        {
            /// <summary>
            /// Gets or sets the start of the range for the delete operation.
            /// Data points from this timestamp (inclusive) will be considered for deletion.
            /// If <c>null</c>, the range starts from the beginning of the time series.
            /// </summary>
            public DateTime? From { get; set; }

            /// <summary>
            /// Gets or sets the end of the range for the delete operation.
            /// Data points up to this timestamp (inclusive) will be considered for deletion.
            /// If <c>null</c>, the range extends to the end of the time series.
            /// </summary>
            public DateTime? To { get; set; }

            internal static DeleteOperation Parse(BlittableJsonReaderObject input)
            {
                input.TryGet(nameof(From), out DateTime? from); // optional
                input.TryGet(nameof(To), out DateTime? to); // optional

                return new DeleteOperation
                {
                    From = from,
                    To = to
                };
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(From)] = From,
                    [nameof(To)] = To
                };
            }
        }

        /// <summary>
        /// Represents an increment operation in a time series, allowing values at a specific timestamp to be incremented.
        /// </summary>
        public sealed class IncrementOperation
        {
            /// <summary>
            /// Gets or sets the timestamp of the data point to be incremented.
            /// This specifies the exact point in time where the values should be adjusted.
            /// </summary>
            public DateTime Timestamp { get; set; }

            /// <summary>
            /// Gets or sets the values to increment at the specified <see cref="Timestamp"/>.
            /// Each value corresponds to a numeric field in the time series, and the increment operation adds the specified amount to the current values.
            /// </summary>
            public double[] Values { get; set; }

            internal int? ValuesLength;

            internal static IncrementOperation Parse(BlittableJsonReaderObject input)
            {
                if (input.TryGet(nameof(Timestamp), out DateTime ts) == false)
                    throw new InvalidDataException($"Missing '{nameof(Timestamp)}' property");

                if (input.TryGet(nameof(Values), out BlittableJsonReaderArray values) == false || values == null)
                    throw new InvalidDataException($"Missing '{nameof(Values)}' property");

                var doubleValues = new double[values.Length];
                for (int i = 0; i < doubleValues.Length; i++)
                {
                    doubleValues[i] = values.GetByIndex<double>(i);
                }

                var op = new IncrementOperation
                {
                    Timestamp = ts,
                    Values = doubleValues
                };

                return op;
            }

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(Timestamp)] = Timestamp,
                    [nameof(Values)] = new DynamicJsonArray(Values)
                };

                if (ValuesLength.HasValue)
                    djv[nameof(ValuesLength)] = ValuesLength;

                return djv;
            }
        }
    }
}
