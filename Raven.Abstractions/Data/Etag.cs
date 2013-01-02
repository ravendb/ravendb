using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Linq;

namespace Raven.Abstractions.Data
{
    public class Etag : IEquatable<Etag>, IComparable<Etag>
    {
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

        private Etag()
        {
            
        }

        public Etag(UuidType type, long restarts, long changes)
        {
            this.restarts = ((long)type << 56) | restarts;
            this.changes = changes;
        }

        public bool Equals(Etag other)
        {
            return other.changes == changes && other.restarts == restarts;
        }

        public int CompareTo(Etag other)
        {
            var sub = restarts - other.restarts;
            if (sub != 0)
                return sub > 0 ? 1 : -1;
            sub = changes - other.changes;
            if (sub != 0)
                return sub > 0 ? 1 : -1;
            return 0;
        }

        public IEnumerable<byte> ToBytes()
        {
            foreach (var source in BitConverter.GetBytes(restarts).Reverse())
            {
                yield return source;
            }
            foreach (var source in BitConverter.GetBytes(changes).Reverse())
            {
                yield return source;
            }
        }

        public byte[] ToByteArray()
        {
            return ToBytes().ToArray();
        }

        public override string ToString()
        {
            var sb = new StringBuilder(36);
            foreach (var by in ToBytes())
            {
                sb.Append(by.ToString("X2"));
            }
            sb.Insert(8, "-")
                    .Insert(13, "-")
                    .Insert(18, "-")
                    .Insert(23, "-");
            return sb.ToString();
        }

        public static Etag Parse(byte[] bytes)
        {
            return new Etag
            {
                restarts = BitConverter.ToInt64(bytes.Take(8).Reverse().ToArray(), 0),
                changes = BitConverter.ToInt64(bytes.Skip(8).Reverse().ToArray(), 0)
            };
        }

        public static Etag Parse(string str)
        {
            if (string.IsNullOrEmpty(str))
                throw new ArgumentException("str cannot be empty or null");
            if (str.Length != 36)
                throw new ArgumentException("str must be 36 characters");

            var buffer = new byte[16]
                                {
                                        byte.Parse(str.Substring(16,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(14,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(11,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(9,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(6,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(4,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(2,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(0,2), NumberStyles.HexNumber),

                                        byte.Parse(str.Substring(34,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(32,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(30,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(28,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(26,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(24,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(21,2), NumberStyles.HexNumber),
                                        byte.Parse(str.Substring(19,2), NumberStyles.HexNumber),
                                };

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
            get { return new Etag
            {
                restarts = 0,
                changes = 0
            }; }
        }

        public Etag IncrementBy(int amount)
        {
            return new Etag
            {
                restarts = restarts,
                changes = changes + amount
            };
        }
    }

    //public class Etag : IComparable, IComparable<Etag>
    //{
    //    public Etag()
    //    {
            
    //    }
    //    public Etag(string s)
    //    {
    //        Value = s;
    //    }

    //    public Etag(Guid guid)
    //    {
    //        Value = guid.ToString();
    //    }

    //    public Etag(Guid? guid)
    //    {
    //        if (guid != null)
    //            Value = guid.ToString();
    //    }

    //    public string Value { get; private set; }

    //    //public static implicit operator string(Etag etag)
    //    //{
    //    //    return etag.Value;
    //    //}

    //    //public override string ToString()
    //    //{
    //    //    return Value;
    //    //}

    //    //public static implicit operator Guid(Etag etag)
    //    //{
    //    //    return new Guid(etag.Value);
    //    //}
    //    //public static implicit operator Guid?(Etag etag)
    //    //{
    //    //    if (string.IsNullOrWhiteSpace(etag.Value))
    //    //        return null;
    //    //    return new Guid(etag.Value);
    //    //}

    //    //public static implicit operator Etag(Guid guid)
    //    //{
    //    //    return new Etag(guid);
    //    //}

    //    //public static implicit operator Etag(Guid? guid)
    //    //{
    //    //    return new Etag(guid);
    //    //}

    //    //public static implicit operator Etag(string s)
    //    //{
    //    //    return new Etag(s);
    //    //}
    //    public int CompareTo(object obj)
    //    {
    //        var etag = obj as string;
    //        if (etag == null)
    //            return 1;
    //        return Value.CompareTo(etag);
    //    }

    //    public int CompareTo(Etag other)
    //    {
    //        if (other == null)
    //            return 1;
    //        return Value.CompareTo(other.Value);
    //    }

    //    public byte[] ToByteArray()
    //    {
    //        return Encoding.UTF8.GetBytes(Value);
    //    }
    //}
}
