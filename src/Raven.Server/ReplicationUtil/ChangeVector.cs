using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ReplicationUtil
{
    public enum ChangeVectorCompareStatus : short
    {
        Greater = 1,
        Lesser,
        Equal,
        Conflict
    }

    public class ChangeVector : IEnumerable<KeyValuePair<string,long>>, IEquatable<ChangeVector>
    {
        private readonly Dictionary<string,long> _etagsByServerIDs;

        public ChangeVector()
        {
            _etagsByServerIDs = new Dictionary<string, long>();
        }

        public ChangeVector(Dictionary<string, long> etagsByServerIDs)
        {
            _etagsByServerIDs = etagsByServerIDs;
        }

        public static ChangeVector FromBlittable(JsonOperationContext context, BlittableJsonReaderObject blittable)
        {
            var fromBlittable = new ChangeVector();
            foreach (var prop in blittable.GetPropertyNames())
            {
                long val;
                if (blittable.TryGet(prop, out val))
                    fromBlittable[prop] = val;
            }

            return fromBlittable;
        }

        public BlittableJsonReaderObject ToBlittable(JsonOperationContext context, string id)
        {
            var obj = new DynamicJsonValue();
            
            foreach (var kvp in _etagsByServerIDs)
                obj[kvp.Key] = kvp.Value;

            return context.ReadObject(obj, id);
        }

        public long this[string serverId]
        {
            get
            {
                return _etagsByServerIDs.GetOrAdd(serverId,0);
            }
            set { _etagsByServerIDs[serverId] = value; }
        }				

        public IEnumerator<KeyValuePair<string, long>> GetEnumerator()
        {
            return _etagsByServerIDs.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <exception cref="ArgumentNullException"><paramref name="other"/> is <see langword="null" />.</exception>
        public ChangeVectorCompareStatus Compare(ChangeVector other)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));

            bool atLeastOneHigherEntry = false;
            bool atLeastOneLowerEntry = false;
            foreach (var key in _etagsByServerIDs.Keys.Union(other._etagsByServerIDs.Keys))
            {
                var v1Entry = _etagsByServerIDs.GetOrDefault(key);
                var v2Entry = other._etagsByServerIDs.GetOrDefault(key);

                if (v1Entry > v2Entry && !atLeastOneHigherEntry)
                    atLeastOneHigherEntry = true;
                if (v1Entry < v2Entry && !atLeastOneLowerEntry)
                    atLeastOneLowerEntry = true;
            }

            if (atLeastOneLowerEntry && atLeastOneHigherEntry)
                return ChangeVectorCompareStatus.Conflict;

            if (atLeastOneHigherEntry)
                return ChangeVectorCompareStatus.Greater; //v1 > v2

            return atLeastOneLowerEntry ? 
                ChangeVectorCompareStatus.Lesser : 
                ChangeVectorCompareStatus.Equal;
        }

        public bool Equals(ChangeVector other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Compare(other) == ChangeVectorCompareStatus.Equal;
        }

        public override bool Equals(object other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            if (other is ChangeVector == false) return false;
            return Compare((ChangeVector)other) == ChangeVectorCompareStatus.Equal;
        }

        public override int GetHashCode()
        {
            return _etagsByServerIDs?.GetHashCode() ?? 0;
        }

        public static bool operator ==(ChangeVector left, ChangeVector right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ChangeVector left, ChangeVector right)
        {
            return !Equals(left, right);
        }
    }
}
