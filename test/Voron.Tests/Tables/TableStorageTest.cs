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

        [Schema]
        public sealed class Documents
        {
            [Id(0)]
            public long Etag;
            [Id(1)]
            public string Key;
            [Id(2)]
            public string Collection;
        }

        [Schema]
        public sealed class DocumentData
        {
            [Id(0)]
            public string Data;
        }
    }
}