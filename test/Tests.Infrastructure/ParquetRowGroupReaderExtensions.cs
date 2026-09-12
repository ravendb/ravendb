using System;
using System.Threading.Tasks;
using Parquet;
using Parquet.Schema;

namespace Tests.Infrastructure;

public static class ParquetRowGroupReaderExtensions
{
    /// <summary>
    /// Reads a whole flat column and returns it as an untyped array.
    /// Parquet.Net 6.0 dropped the untyped 'DataColumn' read API in favour of typed,
    /// caller allocated buffers, so we dispatch on the field's CLR type here instead.
    /// Nullable columns come back as 'T?[]', non nullable ones as 'T[]', which is what
    /// the old 'DataColumn.Data' used to hand out.
    /// </summary>
    public static async ValueTask<Array> ReadColumnDataAsync(this ParquetRowGroupReader rowGroupReader, DataField field)
    {
        var count = checked((int)rowGroupReader.RowCount);
        var clrType = field.ClrType;

        // TIME columns are backed by an integer, turn them back into the TimeSpan
        // values that were handed to the ETL in the first place
        if (field is TimeDataField timeField)
        {
            var ticksPerUnit = timeField.Precision switch
            {
                TimeUnitPrecision.Millis => TimeSpan.TicksPerMillisecond,
                TimeUnitPrecision.Micros => TimeSpan.TicksPerMicrosecond,
                _ => 1
            };

            var timeSpans = new TimeSpan[count];
            if (clrType == typeof(int))
            {
                var millis = new int[count];
                await rowGroupReader.ReadAsync<int>(field, millis.AsMemory());
                for (var i = 0; i < count; i++)
                    timeSpans[i] = TimeSpan.FromTicks(millis[i] * ticksPerUnit);
            }
            else
            {
                var units = new long[count];
                await rowGroupReader.ReadAsync<long>(field, units.AsMemory());
                for (var i = 0; i < count; i++)
                    timeSpans[i] = TimeSpan.FromTicks(units[i] * ticksPerUnit);
            }

            return timeSpans;
        }

        // strings are stored as UTF-8, so a string column reports its type as char memory
        if (clrType == typeof(string) || clrType == typeof(ReadOnlyMemory<char>))
        {
            var strings = new string[count];
            await rowGroupReader.ReadAsync(field, strings.AsMemory());
            return strings;
        }

        if (clrType == typeof(byte[]) || clrType == typeof(ReadOnlyMemory<byte>))
        {
            var byteArrays = new byte[count][];
            await rowGroupReader.ReadAsync(field, byteArrays.AsMemory());
            return byteArrays;
        }

        if (clrType == typeof(bool))
            return await ReadValuesAsync<bool>(rowGroupReader, field, count);
        if (clrType == typeof(byte))
            return await ReadValuesAsync<byte>(rowGroupReader, field, count);
        if (clrType == typeof(sbyte))
            return await ReadValuesAsync<sbyte>(rowGroupReader, field, count);
        if (clrType == typeof(short))
            return await ReadValuesAsync<short>(rowGroupReader, field, count);
        if (clrType == typeof(ushort))
            return await ReadValuesAsync<ushort>(rowGroupReader, field, count);
        if (clrType == typeof(int))
            return await ReadValuesAsync<int>(rowGroupReader, field, count);
        if (clrType == typeof(uint))
            return await ReadValuesAsync<uint>(rowGroupReader, field, count);
        if (clrType == typeof(long))
            return await ReadValuesAsync<long>(rowGroupReader, field, count);
        if (clrType == typeof(ulong))
            return await ReadValuesAsync<ulong>(rowGroupReader, field, count);
        if (clrType == typeof(float))
            return await ReadValuesAsync<float>(rowGroupReader, field, count);
        if (clrType == typeof(double))
            return await ReadValuesAsync<double>(rowGroupReader, field, count);
        if (clrType == typeof(decimal))
            return await ReadValuesAsync<decimal>(rowGroupReader, field, count);
        if (clrType == typeof(DateTime))
            return await ReadValuesAsync<DateTime>(rowGroupReader, field, count);
        if (clrType == typeof(DateOnly))
            return await ReadValuesAsync<DateOnly>(rowGroupReader, field, count);
        if (clrType == typeof(Guid))
            return await ReadValuesAsync<Guid>(rowGroupReader, field, count);

        throw new NotSupportedException($"Unsupported column type '{clrType}' on field '{field.Name}'");
    }

    private static async ValueTask<Array> ReadValuesAsync<T>(ParquetRowGroupReader rowGroupReader, DataField field, int count)
        where T : struct
    {
        if (field.IsNullable)
        {
            var nullableValues = new T?[count];
            await rowGroupReader.ReadAsync<T>(field, nullableValues.AsMemory());
            return nullableValues;
        }

        var values = new T[count];
        await rowGroupReader.ReadAsync<T>(field, values.AsMemory());
        return values;
    }
}
