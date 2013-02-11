//-----------------------------------------------------------------------
// <copyright file="DataConversion.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Provides dictionaries that convert from ESENT column data to objects
    /// and from objects to ESE column data.
    /// </summary>
    internal class DataConversion
    {
        /// <summary>
        /// Initializes a new instance of the DataConversion class.
        /// </summary>
        /// <param name="textEncoding">The Encoding to use for normal (Unicode) text.</param>
        /// <param name="asciiTextEncoding">The Encoding to use for ASCII text.</param>
        public DataConversion(Encoding textEncoding, Encoding asciiTextEncoding)
        {
            this.TextEncoding = textEncoding;
            this.AsciiTextEncoding = asciiTextEncoding;

            this.InitializeConvertToObject();
            this.InitializeConvertObjectToBytes();
            this.InitializeConvertBytesToObject();
        }

        /// <summary>
        /// Gets a dictionary of functions that convert objects into objects of the appropriate type for the given
        /// column.
        /// </summary>
        public Dictionary<ColumnType, Converter<object, object>> ConvertToObject { get; private set; }

        /// <summary>
        /// Gets a dictionary of functions that convert objects of the appropriate type to a byte array suitable
        /// for storing in ESE. The functions in the ConvertToObject dictionary can be used to to convert
        /// an object to a object type that can be used with these functions.
        /// </summary>
        public Dictionary<ColumnType, Converter<object, byte[]>> ConvertObjectToBytes { get; private set; }

        /// <summary>
        /// Gets a dictionary of functions that converts byte arrays to an object of the appropriate type
        /// for the column.
        /// </summary>
        public Dictionary<ColumnType, Converter<byte[], object>> ConvertBytesToObject { get; private set; }

        /// <summary>
        /// Gets or sets the encoding used for text columns.
        /// </summary>
        private Encoding TextEncoding { get; set; }

        /// <summary>
        /// Gets or sets the encoding using for ASCII text columns.
        /// </summary>
        private Encoding AsciiTextEncoding { get; set; }

        /// <summary>
        /// Return a function that calls the given converter function if the data is non-null.
        /// </summary>
        /// <param name="converter">The converter to call for non-null data.</param>
        /// <returns>A Converter that returns null or the results of the input conversion function.</returns>
        private static Converter<object, object> MakeConvertToObjectIfNonNull(Converter<object, object> converter)
        {
            return obj => (null == obj) ? null : converter(obj);
        }

        /// <summary>
        /// Returns a function that converts data into a nullable type.
        /// </summary>
        /// <typeparam name="T">The type to convert to.</typeparam>
        /// <param name="converter">
        /// The conversion function to be used on non-null data. This should produce a struct of type T.
        /// </param>
        /// <returns>A conversion function that returns a Nullable object.</returns>
        private static Converter<object, object> MakeNullableObjectConverter<T>(Converter<object, T> converter) where T : struct
        {
            return obj =>
            {
                if (null != obj)
                {
                    return new Nullable<T>(converter(obj));
                }

                return new Nullable<T>();
            };
        }

        /// <summary>
        /// Return a function that calls the given converter function if the data is non-null.
        /// </summary>
        /// <typeparam name="T">The type of the input object.</typeparam>
        /// <param name="converter">The converter to call for non-null data.</param>
        /// <returns>A Converter that returns null or the results of the input conversion function.</returns>
        private static Converter<object, byte[]> MakeConvertToBytesIfNonNull<T>(Converter<T, byte[]> converter)
        {
            return obj => (null == obj) ? null : converter((T)obj);
        }

        /// <summary>
        /// Returns a function that converts data into a nullable type.
        /// </summary>
        /// <typeparam name="T">The type to convert to.</typeparam>
        /// <param name="converter">
        /// The conversion function to be used on non-null data. This should produce a struct of type T.
        /// </param>
        /// <returns>A conversion function that returns a Nullable object.</returns>
        private static Converter<byte[], object> MakeNullableByteConverter<T>(Converter<byte[], T> converter) where T : struct
        {
            return obj =>
            {
                if (null != obj)
                {
                    return new Nullable<T>(converter(obj));
                }

                return new Nullable<T>();
            };
        }

        /// <summary>
        /// Convert an object to a guid. An exception is thrown if the conversion cannot
        /// be performed.
        /// </summary>
        /// <param name="obj">The object to convert.</param>
        /// <returns>The object as a guid.</returns>
        private static Guid ConvertToGuid(object obj)
        {
            if (obj is Guid)
            {
                return (Guid)obj;
            }

            var s = obj as string;
            if (null != s)
            {
                return new Guid(s);
            }

            var bytes = obj as byte[];
            if (null != bytes)
            {
                return new Guid(bytes);
            }

            throw new InvalidCastException("Unable to convert to a System.Guid");
        }

        /// <summary>
        /// Convert an object to a byte array. An exception is thrown if the conversion cannot
        /// be performed.
        /// </summary>
        /// <param name="obj">The object to convert.</param>
        /// <returns>The object as a byte array.</returns>
        private static byte[] ConvertToByteArray(object obj)
        {
            var bytes = obj as byte[];
            if (null != bytes)
            {
                return bytes;
            }

            throw new InvalidCastException("Unable to convert to byte[]");
        }

        /// <summary>
        /// Initialize the ConvertToObject dictionary.
        /// </summary>
        private void InitializeConvertToObject()
        {
            this.ConvertToObject = new Dictionary<ColumnType, Converter<object, object>>
            {
                { ColumnType.Binary, MakeConvertToObjectIfNonNull(ConvertToByteArray) },
                { ColumnType.Bool, MakeNullableObjectConverter(obj => Convert.ToBoolean(obj)) },
                { ColumnType.Byte, MakeNullableObjectConverter(obj => Convert.ToByte(obj)) },
                { ColumnType.DateTime, MakeNullableObjectConverter(obj => Convert.ToDateTime(obj)) },
                { ColumnType.Double, MakeNullableObjectConverter(obj => Convert.ToDouble(obj)) },
                { ColumnType.Float, MakeNullableObjectConverter(obj => Convert.ToSingle(obj)) },
                { ColumnType.Guid, MakeNullableObjectConverter(obj => ConvertToGuid(obj)) },
                { ColumnType.Int32, MakeNullableObjectConverter(obj => Convert.ToInt32(obj)) },
                { ColumnType.Int64, MakeNullableObjectConverter(obj => Convert.ToInt64(obj)) },
                { ColumnType.Int16, MakeNullableObjectConverter(obj => Convert.ToInt16(obj)) },
                { ColumnType.Text, MakeConvertToObjectIfNonNull(obj => obj.ToString()) },
                { ColumnType.AsciiText, MakeConvertToObjectIfNonNull(obj => obj.ToString()) },
                { ColumnType.UInt32, MakeNullableObjectConverter(obj => Convert.ToUInt32(obj)) },
                { ColumnType.UInt16, MakeNullableObjectConverter(obj => Convert.ToUInt16(obj)) },
            };
        }

        /// <summary>
        /// Initialize the ObjectToBytes dictionary.
        /// </summary>
        private void InitializeConvertObjectToBytes()
        {
            this.ConvertObjectToBytes = new Dictionary<ColumnType, Converter<object, byte[]>>
            {
                { ColumnType.Binary, MakeConvertToBytesIfNonNull<byte[]>(obj => obj) },
                { ColumnType.Bool, MakeConvertToBytesIfNonNull<bool>(BitConverter.GetBytes) },
                { ColumnType.Byte, MakeConvertToBytesIfNonNull<byte>(obj => new[] { obj }) },
                { ColumnType.DateTime, MakeConvertToBytesIfNonNull<DateTime>(obj => BitConverter.GetBytes(obj.ToOADate())) },
                { ColumnType.Double, MakeConvertToBytesIfNonNull<double>(BitConverter.GetBytes) },
                { ColumnType.Float, MakeConvertToBytesIfNonNull<float>(BitConverter.GetBytes) },
                { ColumnType.Guid, MakeConvertToBytesIfNonNull<Guid>(obj => obj.ToByteArray()) },
                { ColumnType.Int32, MakeConvertToBytesIfNonNull<int>(BitConverter.GetBytes) },
                { ColumnType.Int64, MakeConvertToBytesIfNonNull<long>(BitConverter.GetBytes) },
                { ColumnType.Int16, MakeConvertToBytesIfNonNull<short>(BitConverter.GetBytes) },
                { ColumnType.Text, MakeConvertToBytesIfNonNull<string>(obj => this.TextEncoding.GetBytes(obj)) },
                { ColumnType.AsciiText, MakeConvertToBytesIfNonNull<string>(obj => this.AsciiTextEncoding.GetBytes(obj)) },
                { ColumnType.UInt32, MakeConvertToBytesIfNonNull<uint>(BitConverter.GetBytes) },
                { ColumnType.UInt16, MakeConvertToBytesIfNonNull<ushort>(BitConverter.GetBytes) },
            };
        }

        /// <summary>
        /// Initialize the the ConvertBytesToObject dictionary.
        /// </summary>
        private void InitializeConvertBytesToObject()
        {
            this.ConvertBytesToObject = new Dictionary<ColumnType, Converter<byte[], object>>
                                            {
                { ColumnType.Binary, value => value },
                { ColumnType.Bool, MakeNullableByteConverter(value => BitConverter.ToBoolean(value, 0)) },
                { ColumnType.Byte, MakeNullableByteConverter(value => value[0]) },
                { ColumnType.DateTime, MakeNullableByteConverter(value => DateTime.FromOADate(BitConverter.ToDouble(value, 0))) },
                { ColumnType.Double, MakeNullableByteConverter(value => BitConverter.ToDouble(value, 0)) },
                { ColumnType.Float, MakeNullableByteConverter(value => BitConverter.ToSingle(value, 0)) },
                { ColumnType.Guid, MakeNullableByteConverter(value => new Guid(value)) },
                { ColumnType.Int32, MakeNullableByteConverter(value => BitConverter.ToInt32(value, 0)) },
                { ColumnType.Int64, MakeNullableByteConverter(value => BitConverter.ToInt64(value, 0)) },
                { ColumnType.Int16, MakeNullableByteConverter(value => BitConverter.ToInt16(value, 0)) },
                { ColumnType.Text, values => (null == values) ? null : this.TextEncoding.GetString(values) },
                { ColumnType.AsciiText, values => (null == values) ? null : this.AsciiTextEncoding.GetString(values) },
                { ColumnType.UInt32, MakeNullableByteConverter(value => BitConverter.ToUInt32(value, 0)) },
                { ColumnType.UInt16, MakeNullableByteConverter(value => BitConverter.ToUInt16(value, 0)) },
            };
        }
    }
}
