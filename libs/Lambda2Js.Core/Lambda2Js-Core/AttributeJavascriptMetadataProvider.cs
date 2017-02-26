using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using JetBrains.Annotations;

namespace Lambda2Js
{
    /// <summary>
    /// Provides metadata about the objects that are going to be converted to JavaScript in some way.
    /// </summary>
    public class AttributeJavascriptMetadataProvider : JavascriptMetadataProvider
    {
        private IJavascriptMemberMetadata GetMemberMetadataNoCache([NotNull] MemberInfo memberInfo)
        {
            if (memberInfo == null)
                throw new ArgumentNullException(nameof(memberInfo));

            var attr0 = memberInfo
                .GetCustomAttributes(typeof(JavascriptMemberAttribute), true)
                .OfType<IJavascriptMemberMetadata>()
                .SingleOrDefault();

            if (attr0 != null)
                return attr0;

            var jsonAttr = memberInfo
                .GetCustomAttributes(true)
                .Where(a => a.GetType().Name == "JsonPropertyAttribute")
                .Select(ConvertJsonAttribute)
                .SingleOrDefault();

            if (jsonAttr != null)
                return jsonAttr;

            return new JavascriptMemberAttribute
            {
                MemberName = memberInfo.Name
            };
        }

        private readonly Dictionary<MemberInfo, IJavascriptMemberMetadata> cache
            = new Dictionary<MemberInfo, IJavascriptMemberMetadata>();

        private IJavascriptMemberMetadata GetMemberMetadataWithCache([NotNull] MemberInfo memberInfo)
        {
            // ReSharper disable InconsistentlySynchronizedField
            if (!cache.ContainsKey(memberInfo))
                lock (cache)
                    if (!cache.ContainsKey(memberInfo))
                    {
                        var meta = this.GetMemberMetadataNoCache(memberInfo);
                        
                        Interlocked.MemoryBarrier();
                        cache[memberInfo] = meta;
                        return meta;
                    }

            return cache[memberInfo];
            // ReSharper restore InconsistentlySynchronizedField
        }

        /// <summary>
        /// Gets metadata about a property that is going to be used in JavaScript code.
        /// </summary>
        /// <param name="memberInfo"></param>
        /// <returns></returns>
        public override IJavascriptMemberMetadata GetMemberMetadata([NotNull] MemberInfo memberInfo)
        {
            return this.GetMemberMetadata(memberInfo, this.UseCache);
        }

        /// <summary>
        /// Gets or sets a value indicating whether to use cache by default.
        /// Applyes to `GetMemberMetadata` overload without `useCache` parameter.
        /// </summary>
        public bool UseCache { get; set; }

        /// <summary>
        /// Gets metadata about a property that is going to be used in JavaScript code.
        /// </summary>
        /// <param name="memberInfo"></param>
        /// <returns></returns>
        public IJavascriptMemberMetadata GetMemberMetadata([NotNull] MemberInfo memberInfo, bool useCache)
        {
            if (useCache)
                return this.GetMemberMetadataWithCache(memberInfo);

            return this.GetMemberMetadataNoCache(memberInfo);
        }

        private IJavascriptMemberMetadata ConvertJsonAttribute(object attr)
        {
            var type = attr.GetType();
            var accessor = GetAccessors(type);

            return new JavascriptMemberAttribute
            {
                MemberName = accessor.PropertyNameGetter?.Invoke(attr),
            };
        }

        class Accessors
        {
            public Func<object, string> PropertyNameGetter { get; set; }
        }

        private readonly Dictionary<Type, Accessors> accessors = new Dictionary<Type, Accessors>();

        private Accessors GetAccessors(Type type)
        {
            // ReSharper disable InconsistentlySynchronizedField
            if (!accessors.ContainsKey(type))
                lock (accessors)
                    if (!accessors.ContainsKey(type))
                    {
                        var accessor = new Accessors
                        {
                            PropertyNameGetter = type.GetProperty("PropertyName")?.MakeGetterDelegate<string>()
                        };
                        Interlocked.MemoryBarrier();
                        accessors[type] = accessor;
                        return accessor;
                    }

            return accessors[type];
            // ReSharper restore InconsistentlySynchronizedField
        }
    }
}