using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Voron.Util
{
    internal static class Reflection
    {
        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="T">Input type of the lambda.</typeparam>
        /// <typeparam name="TResult">Return type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the MethodInfo for the "int Math::Abs(int)" overload, you can write:
        /// <code>(MethodInfo)Reflection.InfoOf((int x) => Math.Abs(x))</code>
        /// </example>
        static MemberInfo InfoOf<T, TResult>(Expression<Func<T, TResult>> expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            return InfoOf(expression.Body);
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="T">Input type of the lambda.</typeparam>
        /// <typeparam name="TResult">Return type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. Return null if that member is not a method. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the MethodInfo for the "int Math::Abs(int)" overload, you can write:
        /// <code>Reflection.MethodInfoOf((int x) => Math.Abs(x))</code>
        /// </example>
        internal static MethodInfo MethodInfoOf<T, TResult>(Expression<Func<T, TResult>> expression)
        {
            return InfoOf(expression) as MethodInfo;
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="T">Input type of the lambda.</typeparam>
        /// <typeparam name="TResult">Return type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. Return null if the member is not a generic method definition. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the generic method definition for some "int Foo::Bar&lt;T>(T)" overload, you can write:
        /// <code>Reflection.GenericMethodInfoOf((int x) => Foo.Bar(x))</code>, which returns the definition Foo.Bar&lt;>
        /// </example>
        internal static MethodInfo GenericMethodInfoOf<T, TResult>(Expression<Func<T, TResult>> expression)
        {
            var methodInfo = MethodInfoOf(expression);
            return methodInfo == null ? null : methodInfo.GetGenericMethodDefinition();
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="T">Input type of the lambda.</typeparam>
        /// <typeparam name="TResult">Return type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. Return null if the member is not a PropertyInfo. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the PropertyInfo for the "int Foo::SomeProperty", you can write:
        /// <code>Reflection.PropertyInfoOf((Foo f) => f.SomeProperty)</code>
        /// </example>
        internal static PropertyInfo PropertyInfoOf<T, TResult>(Expression<Func<T, TResult>> expression)
        {
            return InfoOf(expression) as PropertyInfo;
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="T">Input type of the lambda.</typeparam>
        /// <typeparam name="TResult">Return type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. Return null if the member is not a FieldInfo. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the FieldInfo for the "int Foo::someField" field, you can write:
        /// <code>Reflection.FieldInfoOf((Foo f) => f.someField)</code>
        /// </example>
        internal static FieldInfo FieldInfoOf<T, TResult>(Expression<Func<T, TResult>> expression)
        {
            return InfoOf(expression) as FieldInfo;
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="TResult">Return type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the PropertyInfo of "DateTime DateTime::Now { get; }", you can write:
        /// <code>(PropertyInfo)Reflection.InfoOf(() => DateTime.Now)</code>
        /// </example>
        static MemberInfo InfoOf<TResult>(Expression<Func<TResult>> expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            return InfoOf(expression.Body);
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="TResult">Return type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. Return null if that member is not a method. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the MethodInfo for the "int Math::Abs(int)" overload, you can write:
        /// <code>Reflection.MethodInfoOf(() => Math.Abs(default(int)))</code>
        /// </example>
        internal static MethodInfo MethodInfoOf<TResult>(Expression<Func<TResult>> expression)
        {
            return InfoOf(expression) as MethodInfo;
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="TResult">Return type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. Return null if the member is not a generic method definition. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the generic method definition for some "int Foo::Bar&lt;T>(T)" overload, you can write:
        /// <code>Reflection.GenericMethodInfoOf(() => Foo.Bar(default(int)))</code>, which returns the definition Foo.Bar&lt;>
        /// </example>
        internal static MethodInfo GenericMethodInfoOf<TResult>(Expression<Func<TResult>> expression)
        {
            var methodInfo = MethodInfoOf(expression);
            return methodInfo == null ? null : methodInfo.GetGenericMethodDefinition();
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="T">Input type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the MethodInfo for the "void Console::WriteLine(string)" overload, you can write:
        /// <code>(MethodInfo)Reflection.InfoOf((string s) => Console.WriteLine(s))</code>
        /// </example>
        static MemberInfo InfoOf<T>(Expression<Action<T>> expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            return InfoOf(expression.Body);
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="T">Input type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. Return null if that member is not a method. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the MethodInfo for the "void Foo::DoThing(int)" overload, you can write:
        /// <code>Reflection.MethodInfoOf(() => Foo.DoThing(default(int)))</code>
        /// </example>
        internal static MethodInfo MethodInfoOf<T>(Expression<Action<T>> expression)
        {
            return InfoOf(expression) as MethodInfo;
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <typeparam name="T">Input type of the lambda.</typeparam>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. Return null if the member is not a generic method definition. An exception occurs if this node does not contain member information.</returns>
        /// <example>
        /// To obtain the generic method definition for some "void Foo::Bar&lt;T>(T)" overload, you can write:
        /// <code>Reflection.GenericMethodInfoOf(() => Foo.Bar(default(int)))</code>, which returns the definition Foo.Bar&lt;>
        /// </example>
        internal static MethodInfo GenericMethodInfoOf<T>(Expression<Action<T>> expression)
        {
            var methodInfo = MethodInfoOf(expression);
            return methodInfo == null ? null : methodInfo.GetGenericMethodDefinition();
        }

        /// <summary>
        /// Gets the reflection member information from the top-level node in the body of the given lambda expression.
        /// </summary>
        /// <param name="expression">Lambda expression to extract reflection information from</param>
        /// <returns>Member information of the top-level node in the body of the lambda expression. An exception occurs if this node does not contain member information.</returns>
        static MemberInfo InfoOf(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            MethodCallExpression mce;
            MemberExpression me;
            NewExpression ne;
            UnaryExpression ue;
            BinaryExpression be;

            if ((mce = expression as MethodCallExpression) != null)
            {
                return mce.Method;
            }
            else if ((me = expression as MemberExpression) != null)
            {
                return me.Member;
            }
            else if ((ne = expression as NewExpression) != null)
            {
                return ne.Constructor;
            }
            else if ((ue = expression as UnaryExpression) != null)
            {
                if (ue.Method != null)
                {
                    return ue.Method;
                }
            }
            else if ((be = expression as BinaryExpression) != null)
            {
                if (be.Method != null)
                {
                    return be.Method;
                }
            }

            throw new NotSupportedException("Expression tree type doesn't have an extractable MemberInfo object.");
        }

        static T GetAttribute<T>(this MemberInfo type)
            where T : class
        {
            return type.GetCustomAttributes(typeof(T), false).FirstOrDefault() as T;
        }

        internal static T GetAttribute<T>(this Type type)
            where T : class
        {
            // ReSharper disable once RedundantCast
            // This explicit cast is needed because when targeting non-portable runtime,
            // type.GetTypeInfo returns an object which is also a Type, causing wrong call.
            return GetAttribute<T>(type.GetTypeInfo() as MemberInfo);
        }

        /// <summary>
        ///     Returns lambda expression body with parameters repalced with passed arguments
        /// </summary>
        public static Expression Inline(LambdaExpression expression, params Expression[] arguments)
        {
            if (arguments.Length != expression.Parameters.Count)
                throw new ArgumentException("Parameter count does not match");
            if (arguments.Length == 1)
                return ReplaceParameter(expression.Body, expression.Parameters[0], arguments[0]);

            var lambdaParams = expression.Parameters.ToList();            
            return ReplaceParameters(expression.Body, p =>  arguments[lambdaParams.FindIndex( x => x.Name == p.Name)]);
        }

        private class ReplaceParametersVisitor : ExpressionVisitor
        {
            private readonly Func<ParameterExpression, Expression> _replace;

            public ReplaceParametersVisitor(Func<ParameterExpression, Expression> replace)
            {
                _replace = replace;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                return _replace(node) ?? base.VisitParameter(node);
            }
        }

        /// <summary>
        ///     Replaces parameter in expression using replace function
        /// </summary>
        public static Expression ReplaceParameters(Expression expression, Func<ParameterExpression, Expression> replace)
        {
            var visitor = new ReplaceParametersVisitor(replace);
            return visitor.Visit(expression);
        }

        /// <summary>
        ///     Replaces parameter in expression to new parameter
        /// </summary>
        public static Expression ReplaceParameter(Expression expression, ParameterExpression parameter, Expression replace)
        {
            return replace == parameter ? expression : ReplaceParameters(expression, p => p == parameter ? replace : p);
        }

    }
}
