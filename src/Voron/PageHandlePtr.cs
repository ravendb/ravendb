using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Voron
{
    public struct PageHandlePtr
    {
        public readonly Page Value;
        public readonly bool IsWritable;

        private const int Invalid = -1;

        public PageHandlePtr(Page value, bool isWritable)
        {
            this.Value = value;
            this.IsWritable = isWritable;
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsValid ? Value.PageNumber : Invalid; }
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Value != null; }
        }
    }
}
