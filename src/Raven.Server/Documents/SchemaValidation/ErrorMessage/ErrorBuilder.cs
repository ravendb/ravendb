using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.SchemaValidation.ErrorMessage;

public class ErrorBuilder : IDisposable
{
    private static readonly string NewErrorDelimiter = Environment.NewLine;
    
    private readonly AbstractBuffer<int> _errorOffsets;
    public readonly IErrorBuffer ErrorBuffer;
        
    public ValidationPath Path { get; }

    public ErrorBuilder()
    {
        var innerBuffer = new RentedBuffer<char>();
        ErrorBuffer = new ErrorBuffer(innerBuffer);
        _errorOffsets = new RentedBuffer<int>();
        Path = new ValidationPath();
    }
    
    public ErrorBuilder(ByteStringContext allocator)
    {
        var innerBuffer = new ByteStringContextBuffer<char>(allocator);
        ErrorBuffer = new ErrorBuffer(innerBuffer);
        _errorOffsets = new ByteStringContextBuffer<int>(allocator);
        Path = new ValidationPath(allocator);
    }
    
    public ErrorBuilder(JsonOperationContext context)
    {
        var innerBuffer = new JsonOperationContextBuffer<char>(context);
        ErrorBuffer = new ErrorBuffer(innerBuffer);
        _errorOffsets = new JsonOperationContextBuffer<int>(context);
        Path = new ValidationPath(context);
    }

    public ReadOnlySpan<char> GetError() => ErrorBuffer.AsSpan();
    
    public IEnumerable<string> GetErrors()
    {
        var span = ErrorBuffer.AsSpan();
        if (span.IsEmpty)
            yield break;

        var startIndex = 0;
        for(var i = 0; i < _errorOffsets.Length; i++)
        {
            var offset = _errorOffsets[i];
            Debug.Assert(offset < ErrorBuffer.Length);

            yield return ErrorBuffer.AsSpan().Slice(startIndex, offset - startIndex).ToString();
            startIndex = offset + NewErrorDelimiter.Length;
        }
        
        if(startIndex < ErrorBuffer.Length)
            yield return ErrorBuffer.AsSpan()[startIndex..].ToString();
    }

    public void FinishErrorMessage()
    {
        _errorOffsets.Append(ErrorBuffer.Length);
        Append(NewErrorDelimiter);
    }

    public ErrorBuilder Append(string value)
    {
        ErrorBuffer.Append(value);
        return this;
    }
    public ErrorBuilder Append(BlittableJsonReaderObject value)
    {
        ErrorBuffer.Append(value);
        return this;
    }
    public ErrorBuilder Append(LazyStringValue value)
    {
        ErrorBuffer.AppendUtf8(value.AsSpan());
        return this;
    }
    public ErrorBuilder Append(ValidationPath value)
    {
        ErrorBuffer.Append(value.AsSpan());
        return this;
    }
    public ErrorBuilder Append(Regex value)
    {
        ErrorBuffer.Append(value.ToString());
        return this;
    }
    public ErrorBuilder Append(bool value)
    {
        ErrorBuffer.Append(value.ToString());
        return this;
    }
    public ErrorBuilder Append(ISpanFormattable value)
    {
        ErrorBuffer.Append(value);
        return this;
    }
    public ErrorBuilder Append(IEnumerable<string> value, string format)
    {
        var first = true;
        foreach (var v in value)
        {
            if (first == false)
                ErrorBuffer.Append(format);
            first = false;
            ErrorBuffer.Append(v);
        }
        return this;
    } 
    public ErrorBuilder Append(IEnumerable<object> value, string format)
    {
        var first = true;
        foreach (var v in value)
        {
            if (first == false)
                ErrorBuffer.Append(format);
            first = false;
            switch (v)
            {
                case string:
                case LazyStringValue:
                case LazyCompressedStringValue:
                    Append('\"').Append(v).Append('\"');
                    break;
                default:
                    Append(v);
                    break;
            }
        }
        return this;
    }
    
    public ErrorBuilder Append(object value)
    {
        switch (value)
        {
            case ISpanFormattable spanFormattable:
                Append(spanFormattable);
                break;
            case BlittableJsonReaderObject blittableValue:
                Append(blittableValue);
                break;
            case LazyNumberValue lazyNumberValue:
                Append((ISpanFormattable)lazyNumberValue.Inner);
                break;
            case ValidationPath validationPathValue:
                Append(validationPathValue);
                break;
            case Regex regexValue:
                Append(regexValue);
                break; 
            case bool boolValue:
                Append(boolValue);
                break;
            case string:
            case null:
                ErrorBuffer.Append((string)value);
                break;
            default:
                ErrorBuffer.Append(value);
                break;
        }

        return this;
    }

    public void Reset()
    {
        ErrorBuffer.Reset();
        _errorOffsets.Reset();
        Path.Reset();
    }

    public LazyStringValue ToLazyStringValue(JsonOperationContext context) => context.GetLazyString(GetError(), null, false);
    
    public override string ToString() => ErrorBuffer?.ToString();
    
    [InterpolatedStringHandler]
    public readonly ref struct ErrorInterpolatedStringHandler
    {
        private readonly ErrorBuilder _errorBuilder;
        public ErrorInterpolatedStringHandler(int literalLength, int formattedCount, ErrorBuilder errorBuilder)
        {
            _errorBuilder = errorBuilder;
        }
        
        public void AppendLiteral(string value) => _errorBuilder.Append(value);
        public void AppendFormatted(string value) => _errorBuilder.Append(value);
        public void AppendFormatted(BlittableJsonReaderObject value) => _errorBuilder.Append(value);
        public void AppendFormatted(LazyStringValue value) => _errorBuilder.Append(value);
        public void AppendFormatted(ValidationPath value) => _errorBuilder.Append(value);
        public void AppendFormatted(Regex value) => _errorBuilder.Append(value);
        public void AppendFormatted(bool value) => _errorBuilder.Append(value);
        public void AppendFormatted(LazyNumberValue value) => _errorBuilder.Append(value);
        public void AppendFormatted(ISpanFormattable value) => _errorBuilder.Append(value);
        public void AppendFormatted(IEnumerable<string> value, string format) => _errorBuilder.Append(value, format);
        public void AppendFormatted(IEnumerable<object> value, string format) => _errorBuilder.Append(value, format);
        public void AppendFormatted(object value) => _errorBuilder.Append(value);
    }

    public void Dispose()
    {
        ErrorBuffer?.Dispose();
        _errorOffsets.Dispose();
        Path.Dispose();
    }
}

public static class ErrorBuilderHelper
{
    public static void AddError(this ErrorBuilder errorBuilder, [InterpolatedStringHandlerArgument("errorBuilder")]ErrorBuilder.ErrorInterpolatedStringHandler message)
    {
        errorBuilder.FinishErrorMessage();
    }
}
