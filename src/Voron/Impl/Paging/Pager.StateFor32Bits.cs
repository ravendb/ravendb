#nullable enable

using System;
using System.Collections.Generic;
using Sparrow;

namespace Voron.Impl.Paging;

public partial class Pager
{
    public class TxStateFor32Bits
    {
        public Dictionary<long, LoadedPage> LoadedPages = [];
        public List<MappedAddresses> AddressesToUnload = [];
        public long TotalLoadedSize;
    }

    public sealed class MappedAddresses
    {
        public string File;
        public IntPtr Address;
        public long StartPage;
        public long Size;
        public int Usages;
    }

    public sealed unsafe class LoadedPage
    {
        public byte* Pointer;
        public int NumberOfPages;
        public long StartPage;
    }

}
