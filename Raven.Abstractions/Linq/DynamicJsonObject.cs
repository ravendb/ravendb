//-----------------------------------------------------------------------
// <copyright file="DynamicJsonObject.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Json.Linq;

namespace Raven.Abstractions.Linq
{
    public interface IDynamicJsonObject
    {
        /// <summary>
        /// Gets the inner json object
        /// </summary>
        /// <value>The inner.</value>
        RavenJObject Inner { get; }

        void WriteTo(JsonWriter writer);
    }
    /// <summary>
    /// A dynamic implementation on top of <see cref="RavenJObject"/>
    /// </summary>
    [JsonObject]
    public class DynamicJsonObject : DynamicObject, IEnumerable<object>, IDynamicJsonObject
    {
        private DynamicJsonObject parent;

        public IEnumerator<object> GetEnumerator()
        {
            return
                (
                    from item in inner
                    where item.Key != JsonTypeReflector.IdPropertyName && 
                          item.Key != JsonTypeReflector.RefPropertyName && 
                          item.Key != JsonTypeReflector.TypePropertyName &&
                          item.Key != JsonTypeReflector.ValuePropertyName &&
                          item.Key != JsonTypeReflector.ArrayValuesPropertyName
                    select new KeyValuePair<object, object>(TransformToValue(item.Key), TransformToValue(item.Value))
                )
                .Cast<object>().GetEnumerator();
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="T:System.Object"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="T:System.Object"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ToString()
        {
            return inner.ToString();
        }

        /// <summary>
        /// Determines whether the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>.
        /// </summary>
        /// <returns>
        /// true if the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>; otherwise, false.
        /// </returns>
        /// <param name="other">The <see cref="T:System.Object"/> to compare with the current <see cref="T:System.Object"/>. </param><filterpriority>2</filterpriority>
        public override bool Equals(object other)
        {
            var dynamicJsonObject = other as DynamicJsonObject;
            if (dynamicJsonObject != null)
                return RavenJToken.DeepEquals(inner, dynamicJsonObject.inner);
            return base.Equals(other);
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>
        /// A hash code for the current <see cref="T:System.Object"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int GetHashCode()
        {
            return RavenJToken.GetDeepHashCode(inner);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private readonly RavenJObject inner;

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicJsonObject"/> class.
        /// </summary>
        /// <param name="inner">The obj.</param>
        public DynamicJsonObject(RavenJObject inner)
        {
            this.inner = inner;
        }


        internal DynamicJsonObject(DynamicJsonObject parent, RavenJObject inner)
        {
            this.parent = parent;
            this.inner = inner;
        }

        /// <summary>
        /// Provides the implementation for operations that get member values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject"/> class can override this method to specify dynamic behavior for operations such as getting a value for a property.
        /// </summary>
        /// <param name="binder">Provides information about the object that called the dynamic operation. The binder.Name property provides the name of the member on which the dynamic operation is performed. For example, for the Console.WriteLine(sampleObject.SampleProperty) statement, where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject"/> class, binder.Name returns "SampleProperty". The binder.IgnoreCase property specifies whether the member name is case-sensitive.</param>
        /// <param name="result">The result of the get operation. For example, if the method is called for a property, you can assign the property value to <paramref name="result"/>.</param>
        /// <returns>
        /// true if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a run-time exception is thrown.)
        /// </returns>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = GetValue(binder.Name);
            if (binder.Name == "HasValue" && result is DynamicNullObject)
            {
                result = false;
                return true;
            }
            return true;
        }

        public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
        {
            if (binder.Name == "Count" && args.Length == 0)
            {
                result = inner.Count;
                return true;
            }
            if (binder.Name == "Value" && args.Length == 1 && args[0] is string)
            {
                result = GetValue((string)args[0]);
                return true;
            }
            return base.TryInvokeMember(binder, args, out result);
        }

        /// <summary>
        /// Provides the implementation for operations that get a value by index. Classes derived from the <see cref="T:System.Dynamic.DynamicObject"/> class can override this method to specify dynamic behavior for indexing operations.
        /// </summary>
        /// <returns>
        /// true if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a run-time exception is thrown.)
        /// </returns>
        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            if (indexes.Length != 1)
            {
                throw new InvalidOperationException("Cannot do indexing with more than a single index, but got " + indexes.Length);
            }
            if (indexes[0] == null)
                throw new InvalidOperationException("Cannot index using a null");

            var token = indexes[0] as RavenJToken;
            if (token != null)
            {
                result = GetValue(token.Value<string>());
                return true;
            }
            if(indexes[0] is string == false)
                throw new InvalidOperationException("Cannot index using " + indexes[0] + " because only strings are supported and it is a " + indexes[0].GetType());
            result = GetValue((string)indexes[0]);
            return true;
        }

        public object TransformToValue(RavenJToken jToken)
        {
            switch (jToken.Type)
            {
                case JTokenType.Object:
                    var jObject = (RavenJObject)jToken;
                    if (jObject.ContainsKey("$values"))
                    {
                        var values = jObject.Value<RavenJArray>("$values");
                        return new DynamicList(this, values.Select(TransformToValue).ToArray());
                    }
                    if (jObject.ContainsKey("$ref"))
                    {
                        var refId = jObject.Value<string>("$ref");
                        var ravenJObject = FindReference(refId);
                        if (ravenJObject != null)
                            return new DynamicJsonObject(this, ravenJObject);
                    }
                    return new DynamicJsonObject(this, jObject);
                case JTokenType.Array:
                    var ar = (RavenJArray)jToken;
                    return new DynamicList(this, ar.Select(TransformToValue).ToArray());
                case JTokenType.Date:
                    var ravenJValue = ((RavenJValue)jToken);
                    return ravenJValue.Value;
                case JTokenType.Null:
                    return new DynamicNullObject { IsExplicitNull = true };
                default:
                    var value = jToken.Value<object>();
                    if (value is long)
                    {
                        var l = (long)value;
                        if (l >= int.MinValue && l <= int.MaxValue)
                            return (int)l;
                    }
                    if (value is Guid)
                    {
                        return value.ToString();
                    }
                    var s = value as string;
                    if (s != null)
                    {
                        //optimizations, don't try to call TryParse if empty
                        if (s.Length == 0)
                            return s;

                        switch (s)
                        {
                            case "NaN":
                                return double.NaN;
                            case "Infinity":
                                return double.PositiveInfinity;
                            case "-Infinity":
                                return double.NegativeInfinity;
                        }

                        //optimizations, don't try to call TryParse if first char isn't a digit or '-'
                        if (char.IsDigit(s[0]) == false && s[0] != '-')
                            return s;


                        DateTime dateTime;
                        if (DateTime.TryParseExact(s, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTime))
                        {
                            if (s.EndsWith("Z"))
                                return DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
                            return dateTime;
                        }
                        DateTimeOffset dateTimeOffset;
                        if (DateTimeOffset.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTimeOffset))
                        {
                            return dateTimeOffset;
                        }
                        TimeSpan timeSpan;
                        if (s.Contains(":") && s.Length >= 6 &&
                            TimeSpan.TryParseExact(s, "c", CultureInfo.InvariantCulture, out timeSpan))
                        {
                            return timeSpan;
                        }

                    }
                    return value ?? new DynamicNullObject { IsExplicitNull = true };
            }
        }

        private RavenJObject FindReference(string refId)
        {
            return GetRootParentOrSelf().Scan().FirstOrDefault(x => x.Value<string>("$id") == refId);
        }

        public DynamicJsonObject GetRootParentOrSelf()
        {
            var p = this;
            while (p.parent != null)
                p = p.parent;
            return p;
        }

        private IEnumerable<RavenJObject> Scan()
        {
            var objs = new List<RavenJObject>();
            var lists = new List<RavenJArray>();

            objs.Add(inner);

            while (objs.Count > 0 || lists.Count > 0)
            {
                var objCopy = objs;
                objs = new List<RavenJObject>();
                foreach (var obj in objCopy)
                {
                    yield return obj;
                    foreach (var property in obj.Properties)
                    {
                        switch (property.Value.Type)
                        {
                            case JTokenType.Object:
                                objs.Add((RavenJObject)property.Value);
                                break;
                            case JTokenType.Array:
                                lists.Add((RavenJArray)property.Value);
                                break;
                        }
                    }
                }

                var listsCopy = lists;
                lists = new List<RavenJArray>();
                foreach (var list in listsCopy)
                {
                    foreach (var item in list)
                    {
                        switch (item.Type)
                        {
                            case JTokenType.Object:
                                objs.Add((RavenJObject)item);
                                break;
                            case JTokenType.Array:
                                lists.Add((RavenJArray)item);
                                break;
                        }
                    }
                }
            }
        }

        public IEnumerable<object> OrderBy(Func<object, object> func)
        {
            return new DynamicList(parent, Enumerable.OrderBy(this, func).ToArray());
        }

        public IEnumerable<object> OrderByDescending(Func<object, object> func)
        {
            return new DynamicList(parent, Enumerable.OrderByDescending(this, func).ToArray());
        }

        public dynamic FirstOrDefault(Func<object, bool> func)
        {
            return Enumerable.FirstOrDefault(this, func) ?? new DynamicNullObject();
        }

        public dynamic First(Func<object, bool> func)
        {
            return FirstOrDefault(func);
        }

        public dynamic LastOrDefault(Func<object, bool> func)
        {
            return Enumerable.LastOrDefault(this, func) ?? new DynamicNullObject();
        }

        public dynamic Last(Func<object, bool> func)
        {
            return LastOrDefault(func);
        }

        public IEnumerable<object> Select(Func<object, object> func)
        {
            return new DynamicList(parent, Enumerable.Select(this, func).ToArray());
        }

        public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func)
        {
            return new DynamicList(parent, Enumerable.SelectMany(this, func).ToArray());
        }

        /// <summary>
        /// Gets the value for the specified name
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        public virtual object GetValue(string name)
        {
            if (name == Constants.DocumentIdFieldName)
            {
                return GetRootParentOrSelf().GetDocumentId();
            }
            RavenJToken value;
            if (inner.TryGetValue(name, out value))
            {
                return TransformToValue(value);
            }
            if (name.StartsWith("_"))
            {
                if (inner.TryGetValue(name.Substring(1), out value))
                {
                    return TransformToValue(value);
                }
            }
            if (name == "Id")
            {
                return GetRootParentOrSelf().GetDocumentId();
            }
            if (name == "Inner")
            {
                return inner;
            }
            if (name == "Count" || name == "Count()")
            {
                return inner.Count;
            }
            return new DynamicNullObject();
        }

        /// <summary>
        /// Gets the document id.
        /// </summary>
        /// <returns></returns>
        public object GetDocumentId()
        {
            var metadata = inner["@metadata"] as RavenJObject;
            if (metadata != null && string.IsNullOrEmpty(metadata.Value<string>("@id")) == false)
            {
                var id = metadata.Value<string>("@id");
                return string.IsNullOrEmpty(id) ? (object)new DynamicNullObject() : id;
            }
            return inner.Value<string>(Constants.DocumentIdFieldName) ?? (object)new DynamicNullObject();
        }


        /// <summary>
        /// Gets the inner json object
        /// </summary>
        /// <value>The inner.</value>
        RavenJObject IDynamicJsonObject.Inner
        {
            get { return inner; }
        }

        public virtual void WriteTo(JsonWriter writer)
        {
            inner.WriteTo(writer);
        }
    }
}
