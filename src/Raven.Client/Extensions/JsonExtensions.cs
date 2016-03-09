//-----------------------------------------------------------------------
// <copyright file="JsonExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

using Raven.Abstractions.Json;
using Raven.Abstractions.Linq;
using  Raven.Imports.Newtonsoft.Json;
using  Raven.Imports.Newtonsoft.Json.Bson;
using  Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Json.Linq;
using System.Collections.Generic;
using Raven.Client.Linq;

namespace Raven.Abstractions.Extensions
{
    /// <summary>
    /// Json extensions 
    /// </summary>
    public static class JsonExtensions
    {
        public static RavenJObject ToJObject(object result)
        {
            var dynamicJsonObject = result as IDynamicJsonObject;
            if (dynamicJsonObject != null)
                return dynamicJsonObject.Inner;
            if (result is string || result is ValueType)
                return new RavenJObject { { "Value", new RavenJValue(result) } };
            if (result is DynamicNullObject)
                return null;
            return RavenJObject.FromObject(result, CreateDefaultJsonSerializer());
        }

        public static RavenJArray ToJArray<T>(IEnumerable<T> result)
        {
            return (RavenJArray) RavenJArray.FromObject(result, CreateDefaultJsonSerializer());
        }

        /// <summary>
        /// Convert a byte array to a RavenJObject
        /// </summary>
        public static RavenJObject ToJObject(this byte[] self)
        {
            using (var stream = new MemoryStream(self))
                return ToJObject(stream);
        }

        /// <summary>
        /// Convert a byte array to a RavenJObject
        /// </summary>
        public static RavenJObject ToJObject(this Stream self)
        {
            var streamWithCachedHeader = new StreamWithCachedHeader(self, 5);
            // note that we intentionally don't close it here
            var jsonReader = new JsonTextReader(new StreamReader(streamWithCachedHeader));
            return RavenJObject.Load(jsonReader);
        }

        /// <summary>
        /// Convert a RavenJToken to a byte array
        /// </summary>
        public static void WriteTo(this RavenJToken self, Stream stream)
        {
            using (var streamWriter = new StreamWriter(stream, Encoding.UTF8, 1024, true))
            using (var jsonWriter = new JsonTextWriter(streamWriter))
            {
                jsonWriter.Formatting = Formatting.None;
                jsonWriter.DateFormatHandling = DateFormatHandling.IsoDateFormat;
                jsonWriter.DateTimeZoneHandling = DateTimeZoneHandling.Utc;
                jsonWriter.DateFormatString = Default.DateTimeFormatsToWrite;
                self.WriteTo(jsonWriter, Default.Converters);
            }
        }

        public static IEnumerable<string> EnumerateJsonObjects(this StreamReader input, char openChar = '{', char closeChar = '}')
        {
            var accumulator = new StringBuilder();
            int count = 0;
            bool gotRecord = false;
            while (!input.EndOfStream)
            {
                char c = (char)input.Read();
                if (c == openChar)
                {
                    gotRecord = true;
                    count++;
                }
                else if (c == closeChar)
                {
                    count--;
                }

                accumulator.Append(c);

                if (count != 0 || !gotRecord)
                    continue;

                // now we are not within a block so 
                string result = accumulator.ToString();
                accumulator.Clear();

                gotRecord = false;

                yield return result;
            }
        }

        /// <summary>
        /// Deserialize a <param name="self"/> to an instance of <typeparam name="T"/>
        /// </summary>
        public static T JsonDeserialization<T>(this byte[] self)
        {
            return (T)CreateDefaultJsonSerializer().Deserialize(new BsonReader(new MemoryStream(self)), typeof(T));
        }

        /// <summary>
        /// Deserialize a <param name="self"/> to an instance of<typeparam name="T"/>
        /// </summary>
        public static T JsonDeserialization<T>(this RavenJToken self)
        {
            return (T)CreateDefaultJsonSerializer().Deserialize(new RavenJTokenReader(self), typeof(T));
        }
        
        /// <summary>
        /// Deserialize a <param name="self"/> to a list of instances of<typeparam name="T"/>
        /// </summary>
        public static T[] JsonDeserialization<T>(this RavenJArray self)
        {
            var serializer = CreateDefaultJsonSerializer();
            return self.Select(x => (T) serializer.Deserialize(new RavenJTokenReader(x), typeof(T))).ToArray();
        }

        /// <summary>
        /// Deserialize a <param name="self"/> to an instance of<typeparam name="T"/>
        /// </summary>
        public static T JsonDeserialization<T>(this StreamReader self)
        {
            return CreateDefaultJsonSerializer().Deserialize<T>(self);
        }
        
        /// <summary>
        /// Deserialize a <param name="stream"/> to an instance of<typeparam name="T"/>
        /// </summary>
        public static T JsonDeserialization<T>(this Stream stream)
        {
            using (var reader = new StreamReader(stream))
            {
                return reader.JsonDeserialization<T>();
            }
        }

        public static T Deserialize<T>(this JsonSerializer self, TextReader reader)
        {
            return (T)self.Deserialize(reader, typeof(T));
        }

        private static readonly IContractResolver contractResolver = new DefaultServerContractResolver()
        {
            DefaultMembersSearchFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
        };

        private class DefaultServerContractResolver : DefaultContractResolver
        {
            protected override System.Collections.Generic.List<MemberInfo> GetSerializableMembers(Type objectType)
            {
                var serializableMembers = base.GetSerializableMembers(objectType);
                foreach (var toRemove in serializableMembers
                    .Where(MembersToFilterOut)
                    .ToArray())
                {
                    serializableMembers.Remove(toRemove);
                }
                return serializableMembers;
            }

            private static bool MembersToFilterOut(MemberInfo info)
            {
                if (info is EventInfo)
                    return true;
                var fieldInfo = info as FieldInfo;
                if (fieldInfo != null && !fieldInfo.IsPublic)
                    return true;
                return info.GetCustomAttributes(typeof(CompilerGeneratedAttribute), true).Any();
            } 
        }

        public static JsonSerializer CreateDefaultJsonSerializer()
        {
            var jsonSerializer = new JsonSerializer
            {
                DateParseHandling = DateParseHandling.None,
                ContractResolver = contractResolver,
                Converters = Default.Converters
            };

            return jsonSerializer;
        }
    }

    internal class StreamWithCachedHeader : Stream
    {
        private readonly Stream inner;

        public int ActualHeaderSize 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get; 
            private set; 
        }

        private int headerSizePosition;

        public byte[] Header 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get; 
            private set; 
        }

        private bool passedHeader;

        private int read;

        public StreamWithCachedHeader(Stream stream, int headerSize)
        {
            inner = stream;
            Header = new byte[headerSize];

            CacheHeader(stream, Header, headerSize);
        }

        private void CacheHeader(Stream stream, byte[] buffer, int headerSize)
        {
            ActualHeaderSize = stream.Read(buffer, 0, headerSize);
        }

        public override void Flush()
        {
            inner.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return inner.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            inner.SetLength(value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (passedHeader)
                return inner.Read(buffer, offset, count);

            if (count <= ActualHeaderSize - headerSizePosition)
            {
                Buffer.BlockCopy(Header, headerSizePosition, buffer, 0, count);
                headerSizePosition += count;
                passedHeader = headerSizePosition >= ActualHeaderSize;
                return count;
            }
            Buffer.BlockCopy(Header, headerSizePosition, buffer, 0, ActualHeaderSize - headerSizePosition);

            var newCount = count - ActualHeaderSize + headerSizePosition;
            var r = inner.Read(buffer, offset + ActualHeaderSize, newCount);

            var currentRead = ActualHeaderSize - headerSizePosition + r;

            read += currentRead;
            headerSizePosition += currentRead;
            passedHeader = read >= ActualHeaderSize;

            return currentRead;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            inner.Write(buffer, offset, count);
        }

        public override bool CanRead
        {
            get
            {
                return inner.CanRead;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return inner.CanSeek;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return inner.CanWrite;
            }
        }

        public override long Length
        {
            get
            {
                return inner.Length;
            }
        }

        public override long Position
        {
            get
            {
                return inner.Position;
            }

            set
            {
                inner.Position = value;
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            inner?.Dispose();
        }
    }
}
