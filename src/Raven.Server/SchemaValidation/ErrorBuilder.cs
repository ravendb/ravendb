using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public class ErrorBuilder : IDisposable
{
    //TODO Maybe writing directly on a stream since we are going to write it back to the respose stream
    private readonly RentedCharBuffer _errorBuffer = new RentedCharBuffer();

    public ValidationPath Path { get; } = new ValidationPath();

    public string GetErrors() => _errorBuffer.Length != 0 ? _errorBuffer.ToString() : null;
    
    public override string ToString() => _errorBuffer?.ToString();
    
    [InterpolatedStringHandler]
    public readonly ref struct ErrorInterpolatedStringHandler
    {
        private readonly RentedCharBuffer _errorBuffer;
        public ErrorInterpolatedStringHandler(int literalLength, int formattedCount, ErrorBuilder errorBuilder)
        {
            _errorBuffer = errorBuilder._errorBuffer;
        }
        
        public void AppendLiteral(string value) => _errorBuffer.Append(value);

        //TODO To avoid allocations when appending BlittableJsonReaderObject
        public void AppendFormatted(BlittableJsonReaderObject value)
        {
            using (var memoryStream = new MemoryStream())
            {
                value.WriteJsonToAsync(memoryStream).AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
                memoryStream.Position = 0;

                var streamReader = new StreamReader(memoryStream);
                _errorBuffer.Read(streamReader);
            }
        }
        public void AppendFormatted(LazyStringValue value) => _errorBuffer.AppendUtf8(value.AsSpan());
        public void AppendFormatted(ValidationPath value) => _errorBuffer.Append(value.AsSpan());
        public void AppendFormatted(Regex value) => _errorBuffer.Append(value.ToString());
        public void AppendFormatted(bool value) => _errorBuffer.Append(value.ToString());
        public void AppendFormatted(LazyNumberValue value) => AppendFormatted(value.Inner);
        
        public void AppendFormatted(IEnumerable<object> value, string format)
        {
            var first = true;
            foreach (var v in value)
            {
                if (first == false)
                    AppendLiteral(format);
                first = false;
                AppendFormatted(v);
            }
        }

        public void AppendFormatted<T>(T value)
        {
            switch (value)
            {
                case BlittableJsonReaderObject blittableValue:
                    AppendFormatted(blittableValue);
                    break;
                case LazyStringValue lazyStringValue:
                    AppendFormatted(lazyStringValue);
                    break;
                case LazyNumberValue lazyNumberValue:
                    AppendFormatted(lazyNumberValue);
                    break;
                case ValidationPath validationPathValue:
                    AppendFormatted(validationPathValue);
                    break;
                case Regex regexValue:
                    AppendFormatted(regexValue);
                    break; 
                case bool boolValue:
                    AppendFormatted(boolValue);
                    break;
                case null:
                    _errorBuffer.Append(null);
                    break;
                default:
                    _errorBuffer.Append(value);
                    break;
            }
        }
    }

    public void Dispose() => _errorBuffer?.Dispose();
}

public static class ErrorBuilderHelper
{
    public static void AddError(this ErrorBuilder errorBuilder, [InterpolatedStringHandlerArgument("errorBuilder")]ErrorBuilder.ErrorInterpolatedStringHandler message)
    {
        message.AppendLiteral(Environment.NewLine);
    }
}
