//-----------------------------------------------------------------------
// <copyright file="MakeKeyHelpers.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Text;

    /// <summary>
    /// Helper methods for the ESENT API. These do data conversion for
    /// JetMakeKey.
        /// </summary>
    public static partial class Api
    {
        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, byte[] data, MakeKeyGrbit grbit)
        {
            if (null == data)
            {
                Api.JetMakeKey(sesid, tableid, null, 0, grbit);
            }
            else if (0 == data.Length)
            {
                Api.JetMakeKey(sesid, tableid, data, data.Length, grbit | MakeKeyGrbit.KeyDataZeroLength);                
            }
            else
            {
                Api.JetMakeKey(sesid, tableid, data, data.Length, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="encoding">The encoding used to convert the string.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, string data, Encoding encoding, MakeKeyGrbit grbit)
        {
            CheckEncodingIsValid(encoding);

            if (null == data)
            {
                Api.JetMakeKey(sesid, tableid, null, 0, grbit);
            }
            else if (0 == data.Length)
            {
                Api.JetMakeKey(sesid, tableid, null, 0, grbit | MakeKeyGrbit.KeyDataZeroLength);
            }
            else if (Encoding.Unicode == encoding)
            {
                // Optimization for Unicode strings
                unsafe
                {
                    fixed (char* buffer = data)
                    {
                        Api.JetMakeKey(sesid, tableid, new IntPtr(buffer), checked(data.Length * sizeof(char)), grbit);
                    }
                }
            }
            else
            {
                // Convert the string using a cached column buffer. The column buffer is far larger
                // than the maximum key size, so any data truncation here won't matter.
                byte[] buffer = Caches.ColumnCache.Allocate();
                int dataSize;
                unsafe
                {
                    fixed (char* chars = data)
                    fixed (byte* bytes = buffer)
                    {
                        dataSize = encoding.GetBytes(chars, data.Length, bytes, buffer.Length);
                    }
                }

                JetMakeKey(sesid, tableid, buffer, dataSize, grbit);
                Caches.ColumnCache.Free(ref buffer);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, bool data, MakeKeyGrbit grbit)
        {
            byte b = data ? (byte)0xff : (byte)0x0;
            Api.MakeKey(sesid, tableid, b, grbit);
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, byte data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(byte);
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, short data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(short);
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, int data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(int);
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, long data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(long);
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, Guid data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = 16 /* sizeof(Guid) */;
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, DateTime data, MakeKeyGrbit grbit)
        {
            Api.MakeKey(sesid, tableid, data.ToOADate(), grbit);
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, float data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(float);
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, double data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(double);
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        [CLSCompliant(false)]
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, ushort data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(ushort);
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        [CLSCompliant(false)]
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, uint data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(uint);
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="JetSeek"/>
        /// and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        [CLSCompliant(false)]
        public static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, ulong data, MakeKeyGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(ulong);
                var pointer = new IntPtr(&data);
                Api.JetMakeKey(sesid, tableid, pointer, DataSize, grbit);
            }
        }
    }
}