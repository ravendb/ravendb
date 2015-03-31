using Raven.Database.Storage.Voron.StorageActions;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Storage.Voron
{
    internal class MockStorageActions : StorageActionsBase
    {
        internal MockStorageActions()
            : base(null, null)
        {

        }

        public static new string CreateKey<T, U, W, X>(T item1, U item2, W item3, X item4)
        {
            return StorageActionsBase.CreateKey(item1, item2, item3, item4);
        }

        public static new string CreateKey<T, U, W>(T item1, U item2, W item3)
        {
            return StorageActionsBase.CreateKey(item1, item2, item3);
        }

        public static new string CreateKey<T, U>(T item1, U item2)
        {
            return StorageActionsBase.CreateKey(item1, item2);
        }

        public static new string CreateKey<T>(T item1)
        {
            return StorageActionsBase.CreateKey(item1);
        }

        public static new string AppendToKey<U, W, X>(string key, U item2, W item3, X item4)
        {
            return StorageActionsBase.AppendToKey(key, item2, item3, item4);
        }

        public static new string AppendToKey<U, W>(string key, U item2, W item3)
        {
            return StorageActionsBase.AppendToKey(key, item2, item3);
        }

        public static new string AppendToKey<U>(string key, U item2)
        {
            return StorageActionsBase.AppendToKey(key, item2);
        }

        public new string CreateKey(object[] values)
        {
            return base.CreateKey(values);
        }

        public new string CreateKey(string[] values)
        {
            return base.CreateKey(values);
        }
    }

    [Trait("VoronTest", "StorageActionsTests")]
    public class StorageActionsTests : NoDisposalNeeded
    {
        [Fact]
        public void CreateKey()
        {
            var s1 = "jSj";
            var s2 = "qwwW";
            var s3 = "12345";
            var s4 = "34kk4";

            var mock = new MockStorageActions();

            Assert.Equal(mock.CreateKey(new object[] { s1 }), MockStorageActions.CreateKey(s1));
            Assert.Equal(mock.CreateKey(new object[] { s1, s2 }), MockStorageActions.CreateKey(s1, s2));
            Assert.Equal(mock.CreateKey(new object[] { s1, s2, s3 }), MockStorageActions.CreateKey(s1, s2, s3));
            Assert.Equal(mock.CreateKey(new object[] { s1, s2, s3, s4 }), MockStorageActions.CreateKey(s1, s2, s3, s4));

            Assert.Equal(mock.CreateKey(new object[] { s1 }), mock.CreateKey(new string[] { s1 }));
            Assert.Equal(mock.CreateKey(new object[] { s1, s2 }), mock.CreateKey(new string[] { s1, s2 }));
            Assert.Equal(mock.CreateKey(new object[] { s1, s2, s3 }), mock.CreateKey(new string[] { s1, s2, s3 }));
            Assert.Equal(mock.CreateKey(new object[] { s1, s2, s3, s4 }), mock.CreateKey(new string[] { s1, s2, s3, s4 }));

            int i1 = 10;
            int i2 = 2;
            int i3 = 30;
            int i4 = 4;

            Assert.Equal(mock.CreateKey(new object[] { i1 }), MockStorageActions.CreateKey(i1));
            Assert.Equal(mock.CreateKey(new object[] { i1, i2 }), MockStorageActions.CreateKey(i1, i2));
            Assert.Equal(mock.CreateKey(new object[] { i1, i2, i3 }), MockStorageActions.CreateKey(i1, i2, i3));
            Assert.Equal(mock.CreateKey(new object[] { i1, i2, i3, i4 }), MockStorageActions.CreateKey(i1, i2, i3, i4));

            Assert.Equal(mock.CreateKey(new object[] { i1 }), mock.CreateKey(new string[] { i1.ToString() }));
            Assert.Equal(mock.CreateKey(new object[] { i1, i2 }), mock.CreateKey(new string[] { i1.ToString(), i2.ToString() }));
            Assert.Equal(mock.CreateKey(new object[] { i1, i2, i3 }), mock.CreateKey(new string[] { i1.ToString(), i2.ToString(), i3.ToString() }));
            Assert.Equal(mock.CreateKey(new object[] { i1, i2, i3, i4 }), mock.CreateKey(new string[] { i1.ToString(), i2.ToString(), i3.ToString(), i4.ToString() }));



            var skey = MockStorageActions.CreateKey(s1);
            var ikey = MockStorageActions.CreateKey(i1);

            Assert.Equal(mock.CreateKey(new object[] { s1, s2 }), MockStorageActions.AppendToKey(skey, s2));
            Assert.Equal(mock.CreateKey(new object[] { s1, s2, s3 }), MockStorageActions.AppendToKey(skey, s2, s3));
            Assert.Equal(mock.CreateKey(new object[] { s1, s2, s3, s4 }), MockStorageActions.AppendToKey(skey, s2, s3, s4));

            Assert.Equal(mock.CreateKey(new object[] { i1, i2 }), MockStorageActions.AppendToKey(ikey, i2));
            Assert.Equal(mock.CreateKey(new object[] { i1, i2, i3 }), MockStorageActions.AppendToKey(ikey, i2, i3));
            Assert.Equal(mock.CreateKey(new object[] { i1, i2, i3, i4 }), MockStorageActions.AppendToKey(ikey, i2, i3, i4));

        }
    }
}
