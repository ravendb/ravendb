// -----------------------------------------------------------------------
//  <copyright file="TableValue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Text;
using Xunit;

using Voron;
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
    }
}