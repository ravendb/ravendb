using System;
using Corax.Utils;
using Sparrow.Json;

namespace Corax.Indexing;

public interface IIndexEntryBuilder
{
    void Boost(float boost);
    ReadOnlySpan<byte> AnalyzeSingleTerm(int fieldId, ReadOnlySpan<byte> value);
    void WriteNull(int fieldId, string path);
    void WriteNonExistingMarker(int fieldId, string path);
    void Write(int fieldId, ReadOnlySpan<byte> value);
    void WriteExactVector(int fieldId, string path, ReadOnlySpan<byte> value);
    void Write(int fieldId, string path, ReadOnlySpan<byte> value);
    void Write(int fieldId, string path, string value);
    void Write(int fieldId, ReadOnlySpan<byte> value, long longValue, double dblValue);
    void Write(int fieldId, string path, string value, long longValue, double dblValue);
    void Write(int fieldId, string path, ReadOnlySpan<byte> value, long longValue, double dblValue);
    void WriteSpatial(int fieldId, string path, CoraxSpatialPointEntry entry);
    void Store(BlittableJsonReaderObject storedValue);
    void Store(int fieldId, string name, BlittableJsonReaderObject storedValue);
    void RegisterEmptyOrNull(int fieldId, string fieldName, StoredFieldType type);
    void IncrementList();
    void DecrementList();
    int ResetList();
    void RestoreList(int old);
}
