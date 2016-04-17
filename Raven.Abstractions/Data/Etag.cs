using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Linq;
using System.Runtime.InteropServices;
using Raven.Abstractions.Util.Encryptors;

namespace Raven.Abstractions.Data
{
    [Serializable]
    public class Etag : IEquatable<Etag>, IComparable<Etag>, IComparable
    {

        public override int GetHashCode()
        {
            unchecked
            {
                return (restarts.GetHashCode() * 397) ^ changes.GetHashCode();
            }
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

        public int CompareTo(object other)
        {
            var otherAsEtag = other as Etag;
            return CompareTo(otherAsEtag);
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

        private IEnumerable<byte> ToBytes()
        {
            return BitConverter.GetBytes(restarts).Reverse()
                .Concat(BitConverter.GetBytes(changes).Reverse());
        }

        public byte[] ToByteArray()
        {
            return ToBytes().ToArray();
        }
        [StructLayout(LayoutKind.Explicit)]
        struct LongBytes
        {
            [FieldOffset(0)]
            public long Long;

            [FieldOffset(0)]
            public Byte Byte0;
            [FieldOffset(1)]
            public Byte Byte1;
            [FieldOffset(2)]
            public Byte Byte2;
            [FieldOffset(3)]
            public Byte Byte3;
            [FieldOffset(4)]
            public Byte Byte4;
            [FieldOffset(5)]
            public Byte Byte5;
            [FieldOffset(6)]
            public Byte Byte6;
            [FieldOffset(7)]
            public Byte Byte7;
        }

        public override unsafe string ToString()
        {
            var results = new string('-', 36);
            // Optimized with the help of Oliver Hallam (oliver.hallam@gmail.com)
            fixed (char* buf = results)
            {
                var bytes = new LongBytes {Long = this.restarts};

                *(int*) (&buf[0]) = ByteToHexStringAsInt32Lookup[bytes.Byte7];
                *(int*) (&buf[2]) = ByteToHexStringAsInt32Lookup[bytes.Byte6];
                *(int*) (&buf[4]) = ByteToHexStringAsInt32Lookup[bytes.Byte5];
                *(int*) (&buf[6]) = ByteToHexStringAsInt32Lookup[bytes.Byte4];

                //buf[8] = '-';
                *(int*) (&buf[9]) = ByteToHexStringAsInt32Lookup[bytes.Byte3];
                *(int*) (&buf[11]) = ByteToHexStringAsInt32Lookup[bytes.Byte2];

                //buf[13] = '-';
                *(int*) (&buf[14]) = ByteToHexStringAsInt32Lookup[bytes.Byte1];
                *(int*) (&buf[16]) = ByteToHexStringAsInt32Lookup[bytes.Byte0];

                //buf[18] = '-';

                bytes.Long = this.changes;

                *(int*) (&buf[19]) = ByteToHexStringAsInt32Lookup[bytes.Byte7];
                *(int*) (&buf[21]) = ByteToHexStringAsInt32Lookup[bytes.Byte6];

                //buf[23] = '-';
                *(int*) (&buf[24]) = ByteToHexStringAsInt32Lookup[bytes.Byte5];
                *(int*) (&buf[26]) = ByteToHexStringAsInt32Lookup[bytes.Byte4];
                *(int*) (&buf[28]) = ByteToHexStringAsInt32Lookup[bytes.Byte3];
                *(int*) (&buf[30]) = ByteToHexStringAsInt32Lookup[bytes.Byte2];
                *(int*) (&buf[32]) = ByteToHexStringAsInt32Lookup[bytes.Byte1];
                *(int*) (&buf[34]) = ByteToHexStringAsInt32Lookup[bytes.Byte0];

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
            if (str == null || str.Length != 36)
                throw new ArgumentException("str cannot be empty or null");
           
            fixed (char* input = str)
            {
                var etag = new Etag();
                int fst = ((byte)(_asciisOfHexToNum[input[0]] * 16 + _asciisOfHexToNum[input[1]])) << 24 |
                    ((byte)(_asciisOfHexToNum[input[2]] * 16 + _asciisOfHexToNum[input[3]])) << 16 |
                    ((byte)(_asciisOfHexToNum[input[4]] * 16 + _asciisOfHexToNum[input[5]])) << 8 |
                    (byte)(_asciisOfHexToNum[input[6]] * 16 + _asciisOfHexToNum[input[7]]);
                int snd = ((byte)(_asciisOfHexToNum[input[9]] * 16 + _asciisOfHexToNum[input[10]])) << 24 |
                    ((byte)(_asciisOfHexToNum[input[11]] * 16 + _asciisOfHexToNum[input[12]])) << 16 |
                    ((byte)(_asciisOfHexToNum[input[14]] * 16 + _asciisOfHexToNum[input[15]])) << 8 |
                    ((byte)(_asciisOfHexToNum[input[16]] * 16 + _asciisOfHexToNum[input[17]]));
                etag.restarts = (uint)snd | ((long)fst << 32);


                fst = ((byte)(_asciisOfHexToNum[input[19]] * 16 + _asciisOfHexToNum[input[20]])) << 24 |
                    ((byte)(_asciisOfHexToNum[input[21]] * 16 + _asciisOfHexToNum[input[22]])) << 16 |
                    ((byte)(_asciisOfHexToNum[input[24]] * 16 + _asciisOfHexToNum[input[25]])) << 8 |
                    ((byte)(_asciisOfHexToNum[input[26]] * 16 + _asciisOfHexToNum[input[27]]));
                snd = ((byte)(_asciisOfHexToNum[input[28]] * 16 + _asciisOfHexToNum[input[29]])) << 24 |
                    ((byte)(_asciisOfHexToNum[input[30]] * 16 + _asciisOfHexToNum[input[31]])) << 16 |
                    ((byte)(_asciisOfHexToNum[input[32]] * 16 + _asciisOfHexToNum[input[33]])) << 8 |
                    ((byte)(_asciisOfHexToNum[input[34]] * 16 + _asciisOfHexToNum[input[35]]));
                etag.changes = (uint)snd | ((long)fst << 32);
                return etag;
            }
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

        public Etag DecrementBy(int amount)
        {
            if(changes< amount)
                throw new ArgumentOutOfRangeException("The etag changes is lower than the given amount");
            return new Etag
            {
                restarts = restarts,
                changes = changes - amount
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
            return string.IsNullOrWhiteSpace(s) ? null : Parse(s);
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

        private static readonly int[] ByteToHexStringAsInt32Lookup;

        static Etag()
        {
            ByteToHexStringAsInt32Lookup = new int[256];
            var abcdef = "0123456789ABCDEF";
            for (var i = 0; i < 256; i++)
            {
                var hex = (abcdef[i / 16] | (abcdef[i % 16] << 16));
                ByteToHexStringAsInt32Lookup[i] = hex;
            }
        }
    }
}
