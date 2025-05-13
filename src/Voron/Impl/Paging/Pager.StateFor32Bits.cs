#nullable enable

using System;
using System.Collections.Generic;
using Sparrow;

namespace Voron.Impl.Paging;

public partial class Pager
{
    public class TxStateFor32Bits(State state)
    {
        public unsafe void* Handle = state.Handle;
        public readonly Dictionary<long, LoadedPage> LoadedPages = [];
        public readonly List<MappedAddresses> AddressesToUnload = [];
        public long TotalLoadedSize;
    }

    public sealed class MappedAddresses(string file, Pager.State state, IntPtr address, long startPage, long size)
    {
        public readonly Pager.State State = state;
        public readonly string File = file;
        public readonly IntPtr Address = address;
        public readonly long StartPage = startPage;
        public readonly long Size = size;
        public int Usages = 1;
    }

    public sealed unsafe class LoadedPage
    {
        public byte* Pointer;
        public int NumberOfPages;
        public long StartPage;
    }

}
