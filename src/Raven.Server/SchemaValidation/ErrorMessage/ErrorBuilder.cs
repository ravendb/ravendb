using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.SchemaValidation.ErrorMessage;

public class ErrorBuilder : IDisposable
{
    private readonly IErrorBuffer _errorBuffer;

    public ValidationPath Path { get; }

    public ErrorBuilder(RentedBuffer<char> innerBuffer)
    {
        _errorBuffer = new ErrorBuffer(innerBuffer);
        Path = new ValidationPath();
    }
    
    public ErrorBuilder()
    {
        var innerBuffer = new RentedBuffer<char>();
        _errorBuffer = new ErrorBuffer(innerBuffer);
        Path = new ValidationPath();
    }
    
    public ErrorBuilder(ByteStringContext allocator)
    {
        var innerBuffer = new ByteStringContextBuffer<char>(allocator);
        _errorBuffer = new ErrorBuffer(innerBuffer);
        Path = new ValidationPath(allocator);
    }
    
    public ErrorBuilder(JsonOperationContext context)
    {
        var innerBuffer = new JsonOperationContextBuffer<char>(context);
        _errorBuffer = new ErrorBuffer(innerBuffer);
        Path = new ValidationPath(context);
    }
    
    public ReadOnlySpan<char> GetErrors() => _errorBuffer.AsSpan();
    
    public void FinishErrorMessage() => Append(Environment.NewLine);

    public ErrorBuilder Append(string value)
    {
        _errorBuffer.Append(value);
        return this;
    }
    public ErrorBuilder Append(BlittableJsonReaderObject value)
    {
        _errorBuffer.Append(value);
        return this;
    }
    public ErrorBuilder Append(LazyStringValue value)
    {
        _errorBuffer.AppendUtf8(value.AsSpan());
        return this;
    }
    public ErrorBuilder Append(ValidationPath value)
    {
        _errorBuffer.Append(value.AsSpan());
        return this;
    }
    public ErrorBuilder Append(Regex value)
    {
        _errorBuffer.Append(value.ToString());
        return this;
    }
    public ErrorBuilder Append(bool value)
    {
        _errorBuffer.Append(value.ToString());
        return this;
    }
    public ErrorBuilder Append(ISpanFormattable value)
    {
        _errorBuffer.Append(value);
        return this;
    }
    public ErrorBuilder Append(IEnumerable<string> value, string format)
    {
        var first = true;
        foreach (var v in value)
        {
            if (first == false)
                _errorBuffer.Append(format);
            first = false;
            _errorBuffer.Append(v);
        }
        return this;
    } 
    public ErrorBuilder Append(IEnumerable<object> value, string format)
    {
        var first = true;
        foreach (var v in value)
        {
            if (first == false)
                _errorBuffer.Append(format);
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
                _errorBuffer.Append((string)value);
                break;
            default:
                _errorBuffer.Append(value);
                break;
        }

        return this;
    }
    
    public override string ToString() => _errorBuffer?.ToString();
    
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

    public void Dispose() => _errorBuffer?.Dispose();
}

public static class ErrorBuilderHelper
{
    public static void AddError(this ErrorBuilder errorBuilder, [InterpolatedStringHandlerArgument("errorBuilder")]ErrorBuilder.ErrorInterpolatedStringHandler message)
    {
        errorBuilder.FinishErrorMessage();
    }
}
