using System;

namespace Sparrow.Json;

public interface IBlittableJsonTextWriter
{
    void WriteObject(BlittableJsonReaderObject obj);
    void WriteValue(BlittableJsonToken token, object val);
    int WriteDateTime(DateTime? value, bool isUtc);
    int WriteDateTime(DateTime value, bool isUtc);
    void WriteString(string str, bool skipEscaping = false);
    void WriteString(LazyStringValue str, bool skipEscaping = false);
    void WriteString(LazyCompressedStringValue str);

    void WriteStartObject();
    void WriteEndObject();

    void WriteStartArray();
    void WriteEndArray();

    

    void WriteNull();
    void WriteBool(bool val);
    void WritePropertyName(ReadOnlySpan<byte> prop);

    void WritePropertyName(string prop);
    void WritePropertyName(StringSegment prop);

    void WriteInteger(long val);
    void WriteDouble(LazyNumberValue val);
    void WriteDouble(double val);

    void WriteNewLine();
    void WriteComma();
}
