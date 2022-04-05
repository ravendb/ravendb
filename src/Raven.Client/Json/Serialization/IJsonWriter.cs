using System;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization
{
    public interface IJsonWriter : IDisposable
    {
        void Close();

        BlittableJsonReaderObject CreateReader();

        void FinalizeDocument();

        void Flush();

        void WriteComment(string text);

        void WriteEnd();

        void WriteEndArray();

        void WriteEndConstructor();

        void WriteEndObject();

        void WriteNull();

        void WriteMetadata(IMetadataDictionary metadata);

        void WritePropertyName(string name);

        void WritePropertyName(string name, bool escape);

        void WriteRaw(string json);

        void WriteRawValue(string json);

        void WriteStartArray();

        void WriteStartConstructor(string name);

        void WriteStartObject();

        void WriteUndefined();

        void WriteValue(bool value);

        void WriteValue(bool? value);

        void WriteValue(byte value);

        void WriteValue(byte? value);

        void WriteValue(byte[] value);

        void WriteValue(char value);

        void WriteValue(char? value);

        void WriteValue(DateTime dt);

        void WriteValue(DateTime? value);

        void WriteValue(DateTimeOffset dto);

        void WriteValue(DateTimeOffset? value);
        
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
        void WriteValue(TimeOnly to);

        void WriteValue(TimeOnly? to);
        
        void WriteValue(DateOnly @do);

        void WriteValue(DateOnly? @do);
#endif
        void WriteValue(decimal value);

        void WriteValue(decimal? value);

        void WriteValue(double value);

        void WriteValue(double? value);

        void WriteValue(float value);

        void WriteValue(float? value);

        void WriteValue(Guid value);

        void WriteValue(Guid? value);

        void WriteValue(int value);

        void WriteValue(int? value);

        void WriteValue(long value);

        void WriteValue(long? value);

        void WriteValue(object value);

        void WriteValue(sbyte value);

        void WriteValue(sbyte? value);

        void WriteValue(short value);

        void WriteValue(short? value);

        void WriteValue(string value);

        void WriteValue(TimeSpan ts);

        void WriteValue(TimeSpan? value);

        void WriteValue(uint value);

        void WriteValue(uint? value);

        void WriteValue(ulong value);

        void WriteValue(ulong? value);

        void WriteValue(Uri value);

        void WriteValue(ushort value);

        void WriteValue(ushort? value);

        void WriteWhitespace(string ws);
    }
}
