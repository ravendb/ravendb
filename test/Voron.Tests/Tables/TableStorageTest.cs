using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Voron.Data.Tables;
using Voron.Util.Conversion;

namespace Voron.Tests.Tables
{

    public class TableStorageTest : StorageTest
    {
        protected TableSchema DocsSchema;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            DocsSchema = new TableSchema("docs")
                .DefineIndex("Etags", new TableSchema.SchemaIndexDef
                {
                    StartIndex = 2,
                    Count = 1,
                    Size = sizeof(long)
                })
                .DefineIndex("Etag&Collection", new TableSchema.SchemaIndexDef
                {
                    StartIndex = 1,
                    Count = 2,
                })
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = 0,
                });
        }

        public unsafe void SetHelper(Table table, params object[] args)
        {
            var builder = new TableValueBuilder();
            var buffers = new List<byte[]>();
            foreach (var o in args)
            {
                var s = o as string;
                if (s != null)
                {
                    buffers.Add(Encoding.UTF8.GetBytes(s));
                    continue;
                }
                var l = (long) o;
                buffers.Add(EndianBitConverter.Big.GetBytes(l));
            }
            var handles1 = new List<GCHandle>();
            foreach (var buffer in buffers)
            {
                var gcHandle1 = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                handles1.Add(gcHandle1);
                builder.Add((byte*) gcHandle1.AddrOfPinnedObject(), buffer.Length);
            }
            var handles = handles1;

            table.Set(builder);

            foreach (var gcHandle in handles)
            {
                gcHandle.Free();
            }
        }
    }
}