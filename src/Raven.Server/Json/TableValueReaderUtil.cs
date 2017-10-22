using System;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron.Data.Tables;

namespace Raven.Server.Json
{
    public static class TableValueReaderUtil
    {
        public static unsafe ReleaseMemory CloneTableValueReader(DocumentsOperationContext context, Table.TableValueHolder read)
        {
            var copyReadMemory = context.GetMemory(read.Reader.Size);
            Memory.Copy(copyReadMemory.Address, read.Reader.Pointer, read.Reader.Size);
            read.Reader = new TableValueReader(copyReadMemory.Address, read.Reader.Size);
            return new ReleaseMemory(context, copyReadMemory);
        }

        public struct ReleaseMemory : IDisposable
        {
            private readonly DocumentsOperationContext _context;
            private readonly AllocatedMemoryData _copyOfMemory;

            public ReleaseMemory(DocumentsOperationContext context, AllocatedMemoryData copyOfMemory)
            {
                _context = context;
                _copyOfMemory = copyOfMemory;
            }

            public void Dispose()
            {
                _context.ReturnMemory(_copyOfMemory);
            }
        }
    }
}
