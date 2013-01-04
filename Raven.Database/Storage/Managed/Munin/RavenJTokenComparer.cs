//-----------------------------------------------------------------------
// <copyright file="JTokenComparer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Munin
{
	public class RavenJTokenComparer : IComparerAndEquality<RavenJToken>
	{
		public static RavenJTokenComparer Instance = new RavenJTokenComparer();

		public virtual int Compare(RavenJToken x, RavenJToken y)
		{
			// null handling
			if (x == null && y == null)
				return 0;
			if (x == null)
				return -1;
			if (y == null)
				return 1;

			if (x.Type != y.Type)
				return (x.Type - y.Type); // this just to force consistent equality

			switch (x.Type)
			{
				case JTokenType.None:
				case JTokenType.Undefined:
				case JTokenType.Null:
					return 0;// all are nil
				case JTokenType.Object:
					// that here we only compare _x_ properties, that is intentional and allows to create partial searches
					// we compare based on _y_ properties order, because that is more stable in our usage
					var xObj = (RavenJObject)x;
					var yObj = (RavenJObject)y;
					foreach (var prop in yObj)
					{
						RavenJToken value;
						if (xObj.TryGetValue(prop.Key, out value) == false)
							continue;
						var compare = Compare(value, prop.Value);
						if (compare != 0)
							return compare;
					}
					return 0;
				case JTokenType.Array:
					var xArray = (RavenJArray)x;
					var yArray = (RavenJArray)y;

					for (int i = 0; i < xArray.Length && i < yArray.Length; i++)
					{
						var compare = Compare(xArray[i], yArray[i]);
						if (compare == 0)
							continue;
						return compare;
					}
					return xArray.Length - yArray.Length;
				//case JTokenType.Property:
				//    var xProp = ((JProperty)x);
				//    var yProp = ((JProperty)y);
				//    var compareTo = xProp.Name.CompareTo(yProp.Name);
				//    if (compareTo != 0)
				//        return compareTo;
				//    return Compare(xProp.Value, yProp.Value);
				case JTokenType.Integer:
					return x.Value<long>().CompareTo(y.Value<long>());
				case JTokenType.Float:
					return (x.Value<double>()).CompareTo(y.Value<double>());
				case JTokenType.String:
					return StringComparer.InvariantCultureIgnoreCase.Compare(x.Value<string>(), y.Value<string>());
				case JTokenType.Boolean:
					return x.Value<bool>().CompareTo(y.Value<bool>());
				case JTokenType.Date:
					return x.Value<DateTime>().CompareTo(y.Value<DateTime>());
				case JTokenType.Bytes:
					var xBytes = x.Value<byte[]>();
					byte[] yBytes = y.Type == JTokenType.String ? Convert.FromBase64String(y.Value<string>()) : y.Value<byte[]>();
					for (int i = 0; i < xBytes.Length && i < yBytes.Length; i++)
					{
						if (xBytes[i] != yBytes[i])
							return xBytes[i] - yBytes[i];
					}
					return xBytes.Length - yBytes.Length;
				case JTokenType.Raw:
				case JTokenType.Comment:
				case JTokenType.Constructor:
					throw new ArgumentOutOfRangeException();
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public bool Equals(RavenJToken x, RavenJToken y)
		{
			return Compare(x, y) == 0;
		}

		public virtual int GetHashCode(RavenJToken obj)
		{
			switch (obj.Type)
			{
				case JTokenType.None:
				case JTokenType.Undefined:
				case JTokenType.Null:
					return 0;
				case JTokenType.Bytes:
					return obj.Value<byte[]>().Aggregate(0, (current, val) => (current * 397) ^ val);
				case JTokenType.Array:
					return ((RavenJArray)obj).Aggregate(0, (current, val) => (current * 397) ^ GetHashCode(val));
				case JTokenType.Object:
					return ((RavenJObject)obj).Aggregate(0, (current, val) => (current * 397) ^ ((val.Key.GetHashCode() * 397) ^ GetHashCode(val.Value)));
				//case JTokenType.Property:
				//    var prop = ((JProperty)obj);
				//    return (prop.Name.GetHashCode() * 397) ^ GetHashCode(prop.Value);
				case JTokenType.Integer:
				case JTokenType.Float:
				case JTokenType.Boolean:
				case JTokenType.Date:
					return ((RavenJValue)obj).Value.GetHashCode();
				case JTokenType.String:
					var jStr = ((RavenJValue)obj);
					if (jStr.Value == null)
						return 0;
					return StringComparer.InvariantCultureIgnoreCase.GetHashCode(jStr.Value<string>());
				case JTokenType.Raw:
				case JTokenType.Comment:
				case JTokenType.Constructor:
					throw new ArgumentOutOfRangeException();
				default:
					throw new ArgumentOutOfRangeException();
			}
		}
	}
}