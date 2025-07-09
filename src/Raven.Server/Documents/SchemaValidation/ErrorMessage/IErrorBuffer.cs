using System;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.ErrorMessage;

public interface IErrorBuffer : IDisposable
{
    ReadOnlySpan<char> AsSpan();
    IErrorBuffer Append(string value);
    IErrorBuffer AppendUtf8(ReadOnlySpan<byte> value);
    IErrorBuffer Append(ReadOnlySpan<char> value);
    IErrorBuffer Append(BlittableJsonReaderObject value);
    IErrorBuffer Append(ISpanFormattable value);
    IErrorBuffer Append(object value);
}
