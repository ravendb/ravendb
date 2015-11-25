using Bond;
using System;
using System.Linq.Expressions;
using System.Text;
using Voron.Data.Tables;

namespace Voron.Tests.Tables
{

    public class TableStorageTest : StorageTest
    {
        protected TableSchema<Documents> _docsSchema;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            _docsSchema = new TableSchema<Documents>("docs")
                                .DefineIndex("By/Etag", x => x.Add(y => y.Etag))
                                .DefineIndex("By/Etag&Collection", x => x.Add(y => y.Collection)
                                                                         .Add(y => y.Etag))
                                .DefineKey( x => x.Add(y => y.Key));
        }

        //private static Slice GetEtagKey(Documents doc)
        //{
        //    var writer = new SliceWriter(sizeof(long));
        //    writer.WriteBigEndian(doc.Etag);
        //    return writer.CreateSlice();
        //}

        //private static Slice GetEtagAndCollectionKey(Documents doc)
        //{
        //    var size = Encoding.UTF8.GetByteCount(doc.Collection);
        //    var writer = new SliceWriter(sizeof(long) + size);
        //    writer.Write(doc.Collection);
        //    writer.WriteBigEndian(doc.Etag);
        //    return writer.CreateSlice();
        //}

        //private static Slice GetKey(Documents doc)
        //{
        //    var size = Encoding.UTF8.GetByteCount(doc.Key);
        //    var writer = new SliceWriter(size);
        //    writer.Write(doc.Key);
        //    return writer.CreateSlice();
        //}

        [Schema]
        public sealed class Documents
        {
            [Id(0)]
            public long Etag;
            [Id(1)]
            public string Key;
            [Id(2)]
            public string Collection;
            [Id(3)]
            public string Data;
        }
    }
}