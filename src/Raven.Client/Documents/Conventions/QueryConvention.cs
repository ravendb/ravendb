using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Client.Documents.Linq;

namespace Raven.Client.Documents.Conventions
{
    public abstract class QueryConventions : Client.Conventions
    {
        public delegate LinqPathProvider.Result CustomQueryTranslator(LinqPathProvider provider, Expression expression);

        private readonly Dictionary<MemberInfo, CustomQueryTranslator> _customQueryTranslators = new Dictionary<MemberInfo, CustomQueryTranslator>();

        /// <summary>
        /// Gets or sets the identity parts separator used by the HiLo generators
        /// </summary>
        /// <value>The identity parts separator.</value>
        public string IdentityPartsSeparator { get; set; }

        /// <summary>
        /// Saves Enums as integers and instruct the Linq provider to query enums as integer values.
        /// </summary>
        public bool SaveEnumsAsIntegers { get; set; }

        public void RegisterCustomQueryTranslator<T>(Expression<Func<T, object>> member, CustomQueryTranslator translator)
        {
            var body = member.Body as UnaryExpression;
            if (body == null)
                throw new NotSupportedException("A custom query translator can only be used to evaluate a simple member access or method call.");

            var info = GetMemberInfoFromExpression(body.Operand);

            if (_customQueryTranslators.ContainsKey(info) == false)
                _customQueryTranslators.Add(info, translator);
        }

        internal LinqPathProvider.Result TranslateCustomQueryExpression(LinqPathProvider provider, Expression expression)
        {
            var member = GetMemberInfoFromExpression(expression);

            CustomQueryTranslator translator;
            return _customQueryTranslators.TryGetValue(member, out translator) == false
                ? null
                : translator.Invoke(provider, expression);
        }

        private static MemberInfo GetMemberInfoFromExpression(Expression expression)
        {
            var callExpression = expression as MethodCallExpression;
            if (callExpression != null)
                return callExpression.Method;

            var memberExpression = expression as MemberExpression;
            if (memberExpression != null)
                return memberExpression.Member;

            throw new NotSupportedException("A custom query translator can only be used to evaluate a simple member access or method call.");
        }
    }
}
