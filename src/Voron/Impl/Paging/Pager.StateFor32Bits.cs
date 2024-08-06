#nullable enable

using System;
using System.Collections.Generic;
using Sparrow;

namespace Voron.Impl.Paging;

public partial class Pager
{
    public class TxStateFor32Bits
    {
        public readonly Dictionary<long, LoadedPage> LoadedPages = [];
        public readonly List<MappedAddresses> AddressesToUnload = [];
        public long TotalLoadedSize;
    }

    public sealed class MappedAddresses(string file, IntPtr address, long startPage, long size)
    {
        public string File = file;
        public IntPtr Address = address;
        public long StartPage = startPage;
        public long Size = size;
        public int Usages = 1;
    }

    public sealed unsafe class LoadedPage
    {
        public byte* Pointer;
        public int NumberOfPages;
        public long StartPage;
    }

}
