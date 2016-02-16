using System;
using System.Collections.Generic;

namespace metrics.Core
{
    /// <summary>
    /// A hash key for storing metrics associated by the parent class and name pair
    /// </summary>
    public struct MetricName : IComparable<MetricName>
    {
        private readonly string _name;
        private readonly string _context;

        public string Name
        {
            get { return _name; }
        }

        public string Context
        {
            get { return _context; }
        }

        public MetricName(string context, string name)
        {
            _name = name;
            _context = context;
        }

        public MetricName(Type @class, string name)
            : this()
        {
            if (@class == null) throw new ArgumentNullException("class");
            if (name == null) throw new ArgumentNullException("name");
            _context = @class.FullName;
            _name = name;
        }

        public bool Equals(MetricName other)
        {
            return string.Equals(Name, other.Name) && string.Equals(Context, other.Context);
        }

        public int CompareTo(MetricName other)
        {
            var r = String.Compare(_context, other._context, StringComparison.OrdinalIgnoreCase);
            if (r != 0)
                return r;
            return String.Compare(_name, other._name, StringComparison.OrdinalIgnoreCase);
        }

        public static bool operator ==(MetricName x, MetricName y)
        {
            return x.CompareTo(y) == 0;
        }

        public static bool operator !=(MetricName x, MetricName y)
        {
            return !(x == y);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is MetricName && Equals((MetricName)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (Context != null ? Context.GetHashCode() : 0);
            }
        }

    }
}



