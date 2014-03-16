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
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Json.Linq;

namespace Raven.Abstractions.Extensions
{
    /// <summary>
    /// Json extensions 
    /// </summary>
    public static class JsonExtensions
    {
        public static RavenJObject ToJObject(object result)
        {
            var dynamicJsonObject = result as Linq.IDynamicJsonObject;
            if (dynamicJsonObject != null)
                return dynamicJsonObject.Inner;
            if (result is string || result is ValueType)
                return new RavenJObject { { "Value", new RavenJValue(result) } };
            return RavenJObject.FromObject(result, CreateDefaultJsonSerializer());
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
            var streamWithCachedHeader = new StreamWithCachedHeader(self, 3);
            if (IsJson(streamWithCachedHeader))
            {
                using (var streamReader = new StreamReader(streamWithCachedHeader, Encoding.UTF8, false, 1024, true))
                using (var jsonReader = new RavenJsonTextReader(streamReader))
                {
                    return RavenJObject.Load(jsonReader);
                }
            }

            return RavenJObject.Load(new BsonReader(streamWithCachedHeader)
            {
                DateTimeKindHandling = DateTimeKind.Utc,
            });
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
        public static T JsonDeserialization<T>(this RavenJObject self)
        {
            return (T)CreateDefaultJsonSerializer().Deserialize(new RavenJTokenReader(self), typeof(T));
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

        private static readonly IContractResolver contractResolver = new DefaultServerContractResolver(shareCache: true)
        {
#if !NETFX_CORE
            DefaultMembersSearchFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
#endif
        };

        private class DefaultServerContractResolver : DefaultContractResolver
        {
            public DefaultServerContractResolver(bool shareCache)
                : base(shareCache)
            {
            }

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
                ContractResolver = contractResolver
            };
            foreach (var defaultJsonConverter in Default.Converters)
            {
                jsonSerializer.Converters.Add(defaultJsonConverter);
            }
            return jsonSerializer;
        }

        private static bool IsJson(StreamWithCachedHeader stream)
        {
            var header = stream.Header;

            if (header[0] == '{')
                return true;

            if (header[0] == 239 && header[1] == 187 && header[2] == 191)
                return true;

            return false;
        }
    }

    internal class StreamWithCachedHeader : Stream
    {
        private readonly Stream inner;

        private readonly int headerSize;

        public byte[] Header { get; private set; }

        private bool passedHeader;

        private int read;

        public StreamWithCachedHeader(Stream stream, int headerSize)
        {
            inner = stream;
            Header = new byte[headerSize];
            this.headerSize = headerSize;

            CacheHeader(stream, Header, headerSize);
        }

        private static void CacheHeader(Stream stream, byte[] buffer, int headerSize)
        {
            stream.Read(buffer, 0, headerSize);
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

            Buffer.BlockCopy(Header, 0, buffer, 0, headerSize);
            if (count <= headerSize)
                return count;

            var newCount = count - headerSize;
            var r = inner.Read(buffer, offset + headerSize, newCount);

            var currentRead = headerSize + r;

            read += currentRead;
            passedHeader = read >= headerSize;

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

        public override void Close()
        {
            if (inner != null)
                inner.Close();
        }
    }
}