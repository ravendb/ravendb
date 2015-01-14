using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Linq;
using System.Security.Cryptography;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Encryptors;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
    [Serializable]
    public class Etag : IEquatable<Etag>, IComparable<Etag>
    {

        public override int GetHashCode()
        {
            unchecked
            {
                return (restarts.GetHashCode() * 397) ^ changes.GetHashCode();
            }
        }

        long restarts;
        long changes;

        public long Restarts
        {
            get { return restarts; }
        }
        public long Changes
        {
            get { return changes; }
        }

        public Etag()
        {

        }

        public Etag(string str)
        {
            var etag = Parse(str);
            restarts = etag.restarts;
            changes = etag.changes;
        }

        public Etag(UuidType type, long restarts, long changes)
        {
            this.restarts = ((long)type << 56) | restarts;
            this.changes = changes;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Etag)obj);
        }

        public static bool operator ==(Etag a, Etag b)
        {
            if (ReferenceEquals(a, null) && ReferenceEquals(b, null))
                return true;
            if (ReferenceEquals(a, null))
                return false;
            return a.Equals(b);
        }

        public static bool operator !=(Etag a, Etag b)
        {
            return !(a == b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Etag other)
        {
            if (null == other) return false;

            return restarts == other.restarts && changes == other.changes;
        }

        public int CompareTo(Etag other)
        {
            if (other == null)
                return -1;
            var sub = restarts - other.restarts;
            if (sub != 0)
                return sub > 0 ? 1 : -1;
            sub = changes - other.changes;
            if (sub != 0)
                return sub > 0 ? 1 : -1;
            return 0;
        }

        private IEnumerable<byte> ToBytes()
        {
            return BitConverter.GetBytes(restarts).Reverse()
                .Concat(BitConverter.GetBytes(changes).Reverse());
        }

        public byte[] ToByteArray()
        {
            return ToBytes().ToArray();
        }

        public unsafe override string ToString()
        {
            var results = new string('-', 36);

            fixed (char* buf = results)
            {
                var buffer = stackalloc byte[8];
                *((long*)buffer) = restarts;
                var duget = GenericUtil.ByteToHexAsStringLookup[buffer[7]];
                buf[0] = duget[0];
                buf[1] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[6]];
                buf[2] = duget[0];
                buf[3] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[5]];
                buf[4] = duget[0];
                buf[5] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[4]];
                buf[6] = duget[0];
                buf[7] = duget[1];
                //buf[8] = '-';
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[3]];
                buf[9] = duget[0];
                buf[10] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[2]];
                buf[11] = duget[0];
                buf[12] = duget[1];
                //buf[13] = '-';
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[1]];
                buf[14] = duget[0];
                buf[15] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[0]];
                buf[16] = duget[0];
                buf[17] = duget[1];
                //buf[18] = '-';

                *((long*)buffer) = changes;

                duget = GenericUtil.ByteToHexAsStringLookup[buffer[7]];
                buf[19] = duget[0];
                buf[20] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[6]];
                buf[21] = duget[0];
                buf[22] = duget[1];
                //buf[23] = '-';
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[5]];
                buf[24] = duget[0];
                buf[25] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[4]];
                buf[26] = duget[0];
                buf[27] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[3]];
                buf[28] = duget[0];
                buf[29] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[2]];
                buf[30] = duget[0];
                buf[31] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[1]];
                buf[32] = duget[0];
                buf[33] = duget[1];
                duget = GenericUtil.ByteToHexAsStringLookup[buffer[0]];
                buf[34] = duget[0];
                buf[35] = duget[1];

                return results;
            }
        }

        public unsafe static Etag Parse(byte[] bytes)
        {
            var etag = new Etag();
            fixed (byte* restarts = bytes)
            {
                int fst = (*restarts << 24) | (*(restarts + 1) << 16) | (*(restarts + 2) << 8) | (*(restarts + 3));
                int snd = (*(restarts + 4) << 24) | (*(restarts + 5) << 16) | (*(restarts + 6) << 8) | (*(restarts + 7));
                etag.restarts = (uint)snd | ((long)fst << 32);

                var changes = restarts + 8;

                fst = (*changes << 24) | (*(changes + 1) << 16) | (*(changes + 2) << 8) | (*(changes + 3));
                snd = (*(changes + 4) << 24) | (*(changes + 5) << 16) | (*(changes + 6) << 8) | (*(changes + 7));
                etag.changes = (uint)snd | ((long)fst << 32);
            }

            return etag;
        }

        public static bool TryParse(string str, out Etag etag)
        {
            try
            {
                etag = Parse(str);
                return true;
            }
            catch (Exception)
            {
                etag = null;
                return false;
            }
        }

        private readonly static int[] _asciisOfHexToNum = CreateHexCharsToNumsTable();

        private static int[] CreateHexCharsToNumsTable()
        {
            var c = new int['z' + 1];
            for (var i = '0'; i <= '9'; i++)
            {
                c[i] = (char)(i - '0');
            }
            for (var i = 'A'; i <= 'Z'; i++)
            {
                c[i] = (char)((i - 'A') + 10);
            }
            for (var i = 'a'; i <= 'z'; i++)
            {
                c[i] = (char)((i - 'a') + 10);
            }

            return c;
        }

        public unsafe static Etag Parse(string str)
        {
            if (string.IsNullOrEmpty(str))
                throw new ArgumentException("str cannot be empty or null");
            if (str.Length != 36)
                throw new ArgumentException(string.Format("str must be 36 characters. Perhaps you are trying to parse non-etag as etag? (string that was passed into Etag::Parse is {0})", str));


            var buffer = new byte[16];

            fixed (char* input = str)
            {
                buffer[0] = (byte)(_asciisOfHexToNum[input[16]] * 16 + _asciisOfHexToNum[input[17]]);
                buffer[1] = (byte)(_asciisOfHexToNum[input[14]] * 16 + _asciisOfHexToNum[input[15]]);
                buffer[2] = (byte)(_asciisOfHexToNum[input[11]] * 16 + _asciisOfHexToNum[input[12]]);
                buffer[3] = (byte)(_asciisOfHexToNum[input[9]] * 16 + _asciisOfHexToNum[input[10]]);
                buffer[4] = (byte)(_asciisOfHexToNum[input[6]] * 16 + _asciisOfHexToNum[input[7]]);
                buffer[5] = (byte)(_asciisOfHexToNum[input[4]] * 16 + _asciisOfHexToNum[input[5]]);
                buffer[6] = (byte)(_asciisOfHexToNum[input[2]] * 16 + _asciisOfHexToNum[input[3]]);
                buffer[7] = (byte)(_asciisOfHexToNum[input[0]] * 16 + _asciisOfHexToNum[input[1]]);
                buffer[8] = (byte)(_asciisOfHexToNum[input[34]] * 16 + _asciisOfHexToNum[input[35]]);
                buffer[9] = (byte)(_asciisOfHexToNum[input[32]] * 16 + _asciisOfHexToNum[input[33]]);
                buffer[10] = (byte)(_asciisOfHexToNum[input[30]] * 16 + _asciisOfHexToNum[input[31]]);
                buffer[11] = (byte)(_asciisOfHexToNum[input[28]] * 16 + _asciisOfHexToNum[input[29]]);
                buffer[12] = (byte)(_asciisOfHexToNum[input[26]] * 16 + _asciisOfHexToNum[input[27]]);
                buffer[13] = (byte)(_asciisOfHexToNum[input[24]] * 16 + _asciisOfHexToNum[input[25]]);
                buffer[14] = (byte)(_asciisOfHexToNum[input[21]] * 16 + _asciisOfHexToNum[input[22]]);
                buffer[15] = (byte)(_asciisOfHexToNum[input[19]] * 16 + _asciisOfHexToNum[input[20]]);
            }

            return new Etag
            {
                restarts = BitConverter.ToInt64(buffer, 0),
                changes = BitConverter.ToInt64(buffer, 8)
            };
        }

        public static Etag InvalidEtag
        {
            get
            {
                return new Etag
                {
                    restarts = -1,
                    changes = -1
                };
            }
        }
        public static Etag Empty
        {
            get
            {
                return new Etag
                    {
                        restarts = 0,
                        changes = 0
                    };
            }
        }

        public Etag Setup(UuidType type, long restartsNum)
        {
            return new Etag
                {
                    restarts = ((long)type << 56) | restartsNum,
                    changes = changes
                };
        }

        public Etag IncrementBy(int amount)
        {
            return new Etag
            {
                restarts = restarts,
                changes = changes + amount
            };
        }

        public static implicit operator string(Etag etag)
        {
            if (etag == null)
                return null;
            return etag.ToString();
        }

        public static implicit operator Etag(string s)
        {
            return Parse(s);
        }

        public static implicit operator Etag(Guid g)
        {
            return GuidToEtag(g);
        }

        public static implicit operator Guid?(Etag e)
        {
            if (e == null)
                return null;
            return EtagToGuid(e);
        }

        public static implicit operator Etag(Guid? g)
        {
            if (g == null)
                return null;
            return GuidToEtag(g.Value);
        }

        public static implicit operator Guid(Etag e)
        {
            return EtagToGuid(e);
        }

        public Etag HashWith(Etag other)
        {
            return HashWith(other.ToBytes());
        }

        public Etag HashWith(IEnumerable<byte> bytes)
        {
            var etagBytes = ToBytes().Concat(bytes).ToArray();

            return Parse(Encryptor.Current.Hash.Compute16(etagBytes));
        }

        public static Etag Max(Etag first, Etag second)
        {
            if (first == null)
                return second;
            return first.CompareTo(second) > 0
                ? first
                : second;
        }

        private static Guid EtagToGuid(Etag etag)
        {
            var bytes = etag.ToByteArray();
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes, 0, 4);
                Array.Reverse(bytes, 4, 2);
                Array.Reverse(bytes, 6, 2);
            }
            return new Guid(bytes);
        }

        private static Etag GuidToEtag(Guid guid)
        {
            var bytes = guid.ToByteArray();
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes, 0, 4);
                Array.Reverse(bytes, 4, 2);
                Array.Reverse(bytes, 6, 2);
            }
            return Etag.Parse(bytes);
        }
    }

    public class EtagJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var etag = value as Etag;
            if (etag == null)
                writer.WriteNull();
            else
                writer.WriteValue(etag.ToString());
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var s = reader.Value as string;
            if (s == null)
                return null;
            return Etag.Parse(s);
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Etag);
        }
    }
}
