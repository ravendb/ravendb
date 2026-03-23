using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Json;

namespace Sparrow.Utils;

public static class StringUtils
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe EscapePositionsReader GetEscapePositionsReader(LazyStringValue lazyStringValue)
    {
        return new EscapePositionsReader(lazyStringValue.Buffer, lazyStringValue.Size);
    }    
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int CountEscapedControlCharacters(ReadOnlySpan<byte> source, Span<int> escapePositions)
    {
        // To identify escaped control characters, we look for the '\uXXXX' sequence.
        // We only count backslashes that are NOT marked in the escape positions.
        // A backslash present in the escape positions list indicates it was part of the original 
        // string (a literal backslash), rather than the start of an escape sequence.
        
        if (escapePositions.Length == 0)
            return CountBackslashes(source);

        var count = 0;
        for (var i = 0; i < escapePositions.Length; i++)
        {
            var next = escapePositions[i];
            if (next > 0)
                count += CountBackslashes(source.Slice(0, next));

            source = source.Slice(next + 1);
        }

        if (source.Length > 0)
            count += CountBackslashes(source);

        return count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountBackslashes(ReadOnlySpan<byte> source)
    {
        const byte escapeChar = (byte)'\\';
        var count = 0;
        int idx;
        while ((idx = source.IndexOf(escapeChar)) != -1)
        {
            count++;
            source = source.Slice(idx + 1);
        }
        return count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Span<byte> UnescapeControlCharacters(ReadOnlySpan<byte> source, Span<int> sourceEscapedPositions, Span<byte> destination, Span<int> destinationEscapedPositions)
    {
        const int unicodeHexLength = 4; // \uXXXX

        var newEscapedPositions = new SpanWriter<int>(destinationEscapedPositions);

        var sourceReader = new SpanReader<byte>(source);
        var destinationWriter = new SpanWriter<byte>(destination);

        var nextEscapedPosition = sourceEscapedPositions.Length > 0 ? sourceEscapedPositions[0] : -1;
        var escapePositionsIndex = 1;
        var lastNewEscapedPos = -1;

        while(sourceReader.Read(out var b))
        {
            if (sourceReader.Position == nextEscapedPosition)
            {
                nextEscapedPosition = escapePositionsIndex < sourceEscapedPositions.Length
                    ? nextEscapedPosition + sourceEscapedPositions[escapePositionsIndex++] + 1
                    : -1;

                newEscapedPositions.Append(destinationWriter.Size - lastNewEscapedPos - 1);
                lastNewEscapedPos = destinationWriter.Size;
            }
            else if (b == (byte)'\\')
            {
                if (sourceReader.Read(out b) == false || b != (byte)'u')
                    throw new InvalidOperationException($@"Invalid escape sequence at index {destinationWriter.Size}: expected '\u' but found '\{(char)b}'");

                if(sourceReader.Read(unicodeHexLength, out var span) == false)
                    throw new InvalidOperationException($@"Invalid escape sequence at index {destinationWriter.Size}: expected {unicodeHexLength} hex characters after '\u'");

                b = ParseUnicodeValue(span);
                
                newEscapedPositions.Append(destinationWriter.Size - lastNewEscapedPos - 1);
                lastNewEscapedPos = destinationWriter.Size;
            }

            destinationWriter.Append(b);
        }

        return destinationWriter.AsSpan();
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ParseUnicodeValue(ReadOnlySpan<byte> buffer)
    {
        Debug.Assert(buffer.Length >= 4);

        var val = 0;
        for (var i = 0; i < 4; i++)
        {
            var b = buffer[i];

            if (b >= (byte)'0' && b <= (byte)'9')
            {
                val = (val << 4) | (b - (byte)'0');
            }
            else if (b >= 'a' && b <= (byte)'f')
            {
                val = (val << 4) | (10 + (b - (byte)'a'));
            }
            else if (b >= 'A' && b <= (byte)'F')
            {
                val = (val << 4) | (10 + (b - (byte)'A'));
            }
            else
            {
                throw new InvalidOperationException($"Invalid hex value '{ (char)b }' (0x{b:X2}) at unicode escape sequence");
            }
        }

        if (val > byte.MaxValue)
            throw new InvalidOperationException($"Unicode value 0x{val:X4} is too large for a byte (max 0xFF)");

        return (byte)val;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CheckAndThrowContainsControlCharacters(ReadOnlySpan<char> id)
    {
        if (HasControlCharacters(id))
            ThrowIdentifierWithControlCharacters(id);
    }

    public static void ThrowIdentifierWithControlCharacters(ReadOnlySpan<char> str)
    {
        throw new NotSupportedException($"Identifier cannot contain control characters : '{EscapeControlCharacters(str)}' (escaped version)");
    }

    private static string EscapeControlCharacters(ReadOnlySpan<char> str)
    {
        var sb = new StringBuilder();
        foreach (var c in str)
        {
            if (IsControlCharacter(c))
            {
                sb.Append("\\u");
                sb.Append(((int)c).ToString("x4"));
                continue;
            }
            sb.Append(c);
        }
        return sb.ToString();
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool HasControlCharacters(ReadOnlySpan<char> str)
    {
        for (var index = 0; index < str.Length; index++)
        {
            if (IsControlCharacter(str[index]))
                return true;
        }

        return false;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsControlCharacter(char c)
    {
        // 8  => '\b' => 0000 1000
        // 9  => '\t' => 0000 1001
        // 10 => '\n' => 0000 1010
        // 12 => '\f' => 0000 1100
        // 13 => '\r' => 0000 1101
        return c < 32 && (c < 8 || c > 13 || c == 11);
    }
    
    public ref struct SpanWriter<T>
    {
        private readonly Span<T> _buffer;

        private int _size;
        
        public int Size 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SpanWriter(Span<T> buffer)
        {
            _buffer = buffer;
            _size = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Append(T value)
        {
            Debug.Assert(_size < _buffer.Length);
            _buffer[_size++] = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan() => _buffer.Slice(0, _size);
    }

    private ref struct SpanReader<T>
    {
        private readonly ReadOnlySpan<T> _buffer;

        public int Position
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            private set;
        } = -1;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SpanReader(ReadOnlySpan<T> buffer)
        {
            _buffer = buffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(out T t)
        {
            ++Position;
            if(Position < _buffer.Length)
            {
                t = _buffer[Position];   
                return true;
                
            }
            t = default;
            return false;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(int i, out ReadOnlySpan<T> span)
        {
            if(Position + i < _buffer.Length)
            {
                var result =  _buffer.Slice(Position + 1, i); 
                Position += i;
                span = result;
                return true;
            }
            span = default;
            return false;
        }
    }
    
    public unsafe struct EscapePositionsReader
    {
        private readonly byte* _buffer;
        private int _offset;
        private int _read;

        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set; 
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
        }
        
        public int Current;

        public EscapePositionsReader(byte* buffer, int size)
        {
            _read = 0;
            Current = -1;

            _buffer = buffer;
            _offset = size;
            Length = BlittableJsonReaderBase.ReadVariableSizeInt(_buffer, ref _offset);
        }

        public bool Read()
        {
            if (_read >= Length)
                return false;

            Current = BlittableJsonReaderBase.ReadVariableSizeInt(_buffer, ref _offset);

            _read++;
            return true;
        }
    }
}
