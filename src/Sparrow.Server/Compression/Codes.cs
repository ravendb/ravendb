using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Binary;

namespace Sparrow.Server.Compression
{
    [StructLayout(LayoutKind.Explicit)]
    internal struct Code
    {
        [FieldOffset(0)]
        public int Value;

        [FieldOffset(4)]
        public sbyte Length;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct Symbol
    {
        private uint _startKey;
        private byte _length;

        public Symbol(in ReadOnlySpan<byte> startKey)
        {
            Debug.Assert(startKey.Length <= 4);

            Span<byte> aux = stackalloc byte[4];
            startKey.CopyTo(aux);
            var intAux = MemoryMarshal.Cast<byte, uint>(aux);
            _startKey = intAux[0];

            _length = (byte)startKey.Length;
        }

        public uint StartKeyAsInt => Bits.SwapBytes(_startKey);

        public Span<byte> StartKey => new(Unsafe.AsPointer(ref _startKey), _length);

        public int Length => _length;

        public override string ToString() => $"({Length},{Encoding.ASCII.GetString(StartKey)})";
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct SymbolCode
    {
        private uint _startKey;
        public int Length;
        public Code Code;

        public SymbolCode(in ReadOnlySpan<byte> startKey, in Code code)
        {
            Debug.Assert(startKey.Length <= 4);

            Span<byte> aux = stackalloc byte[4];
            startKey.CopyTo(aux);
            var intAux = MemoryMarshal.Cast<byte, uint>(aux);
            _startKey = intAux[0];

            Code = code;
            Length = startKey.Length;

            _startKey = 0;
            startKey.CopyTo(StartKey);
        }

        public Span<byte> StartKey => new(Unsafe.AsPointer(ref _startKey), Length);

        public override string ToString() => $"(Code={Code.Length},{Code.Value}|{Length},{Encoding.ASCII.GetString(StartKey)})";
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct SymbolFrequency
    {
        private uint _startKey;
        public int Frequency;
        public int Length;

        public SymbolFrequency(in ReadOnlySpan<byte> startKey, int frequency)
        {
            Debug.Assert(startKey.Length <= 4);

            Span<byte> aux = stackalloc byte[4];
            startKey.CopyTo(aux);
            var intAux = MemoryMarshal.Cast<byte, uint>(aux);
            _startKey = intAux[0];

            Length = startKey.Length;
            Frequency = frequency;
        }

        public ReadOnlySpan<byte> StartKey => new(Unsafe.AsPointer(ref _startKey), Length);

        public override string ToString() => $"(Freq={Frequency}|{Length},{Encoding.ASCII.GetString(StartKey)})";
    }
}
