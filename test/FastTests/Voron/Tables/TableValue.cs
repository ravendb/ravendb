// -----------------------------------------------------------------------
//  <copyright file="TableValue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Text;
using Xunit;

using Voron.Data.Tables;

namespace FastTests.Voron.Tables
{
    public unsafe class TableValueTests
    {
        [Fact]
        public void CanBuild()
        {
            var oren = Encoding.UTF8.GetBytes("Oren Eini");
            var longer = Encoding.UTF8.GetBytes("There is a man on the moon and he's eating cheese");
            var fun = Encoding.UTF8.GetBytes("Writing code that make funny stuff happen is quite a pleasurable activity");
            fixed (byte* pOren = oren)
            {
                fixed (byte* pLonger = longer)
                {
                    fixed (byte* pFun = fun)
                    {
                        var tableValue = new TableValueBuilder
                        {
                            {pOren, oren.Length},
                            {pFun, fun.Length},
                            {pLonger, longer.Length}
                        };

                        Assert.Equal(longer.Length + oren.Length + fun.Length + 3 + 1, tableValue.Size);
                    }
                }
            }
        }

        [Fact]
        public void CanRead()
        {
            var orenStr = "Oren Eini";
            var oren = Encoding.UTF8.GetBytes(orenStr);
            var longerStr = "There is a man on the moon and he's eating cheese";
            var longer = Encoding.UTF8.GetBytes(longerStr);
            var funStr = "Writing code that make funny stuff happen is quite a pleasurable activity";
            var fun = Encoding.UTF8.GetBytes(funStr);
            fixed (byte* pOren = oren)
            {
                fixed (byte* pLonger = longer)
                {
                    fixed (byte* pFun = fun)
                    {
                        var tableValue = new TableValueBuilder
                        {
                            {pOren, oren.Length},
                            {pFun, fun.Length},
                            {pLonger, longer.Length}
                        };

                        fixed (byte* buffer = new byte[tableValue.Size])
                        {
                            tableValue.CopyTo(buffer);

                            var reader = new TableValueReader(buffer, tableValue.Size);
                            int size;
                            var p = reader.Read(2, out size);
                            var actual = Encoding.UTF8.GetString(p, size);
                            Assert.Equal(longerStr, actual);

                            p = reader.Read(0, out size);
                            actual = Encoding.UTF8.GetString(p, size);
                            Assert.Equal(orenStr, actual);

                            p = reader.Read(1, out size);
                            actual = Encoding.UTF8.GetString(p, size);
                            Assert.Equal(funStr, actual);
                        }

                    }
                }
            }
        }

        [Fact]
        public void CanSerializeBool()
        {
            var v1 = true;
            var v2 = false;

            var tableValue = new TableValueBuilder { &v1, &v2 };

            fixed (byte* buffer = new byte[tableValue.Size])
            {
                tableValue.CopyTo(buffer);

                var reader = new TableValueReader(buffer, tableValue.Size);

                int size;
                var p = reader.Read(0, out size);
                var value = Convert.ToBoolean(*p);

                Assert.Equal(1, size);
                Assert.Equal(true, value);

                p = reader.Read(1, out size);
                value = Convert.ToBoolean(*p);

                Assert.Equal(1, size);
                Assert.Equal(false, value);
            }
        }

        [Fact]
        public void CanSerializeRecursively()
        {
            var v1 = 0L;
            var v2 = 1L;
            var innerTableValue = new TableValueBuilder { &v1, &v2 };

            byte[] temporary = new byte[innerTableValue.Size];

            fixed (byte* ptr = temporary)
            {
                innerTableValue.CopyTo(ptr);

                var tableValue = new TableValueBuilder { &v1, { ptr, innerTableValue.Size } };

                fixed (byte* buffer = new byte[tableValue.Size])
                {
                    tableValue.CopyTo(buffer);

                    var reader = new TableValueReader(buffer, tableValue.Size);

                    int size;
                    var p = reader.Read(0, out size);
                    var hello = *(long*) p;

                    Assert.Equal(0L, hello);

                    p = reader.Read(1, out size);

                    // Get into reading the inner Table Value Reader
                    var innerReader = new TableValueReader(p, size);

                    p = innerReader.Read(0, out size);
                    var value = *(long*)p;

                    Assert.Equal(sizeof(long), size);
                    Assert.Equal(0L, value);

                    p = innerReader.Read(1, out size);
                    Assert.Equal(sizeof(long), size);
                    value = *(long*)p;

                    Assert.Equal(1L, value);
                }
            }
        }
    }
}