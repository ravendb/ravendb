using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Exceptions;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.FileSystem;
using Raven.NewClient.Client.Exceptions;
using Newtonsoft.Json;

namespace Raven.NewClient.Client.FileSystem.Extensions
{
    /// <summary>
    ///     Extension methods to handle common scenarios
    /// </summary>
    public static class ExceptionExtensions
    {
        public static Task TryThrowBetterError(this Task self)
        {
            return self.ContinueWith(task =>
            {
                if (task.Status != TaskStatus.Faulted)
                    return task;

                var innerException = task.Exception.ExtractSingleInnerException();

                var errorResponseException = innerException as ErrorResponseException;
                if (errorResponseException != null)
                    throw errorResponseException.SimplifyException();

                throw innerException;
            }).Unwrap();
        }

        public static Exception SimplifyException(this ErrorResponseException errorResposeException)
        {
            if (errorResposeException.StatusCode == (HttpStatusCode) 420)
            {
                var text = errorResposeException.ResponseString;
                var errorResults = JsonConvert.DeserializeAnonymousType(text, new
                {
                    url = (string) null,
                    error = (string) null
                });

                return new SynchronizationException(errorResults.error);
            }

            if (errorResposeException.StatusCode == HttpStatusCode.MethodNotAllowed)
            {
                var text = errorResposeException.ResponseString;
                var errorResults = JsonConvert.DeserializeAnonymousType(text, new
                {
                    url = (string) null,
                    actualETag = 0,
                    expectedETag = 0,
                    error = (string) null
                });

                return new ConcurrencyException(errorResults.error)
                {
                    ActualETag = errorResults.actualETag,
                    ExpectedETag = errorResults.expectedETag
                };
            }
            if (errorResposeException.StatusCode == HttpStatusCode.NotFound)
            {
                var text = errorResposeException.ResponseString;

                if(string.IsNullOrEmpty(text))
                    return new FileNotFoundException();

                var errorResults = JsonConvert.DeserializeAnonymousType(text, new
                {
                    url = (string) null,
                    error = (string) null
                });

                return new FileNotFoundException(errorResults.error);
            }
            if (errorResposeException.StatusCode == HttpStatusCode.BadRequest)
            {
                return new BadRequestException();
            }

            return errorResposeException;
        }

        public static Task<T> TryThrowBetterError<T>(this Task<T> self)
        {
            return self.ContinueWith(task =>
            {
                if (task.Status != TaskStatus.Faulted)
                    return task;

                var innerException = task.Exception.ExtractSingleInnerException();

                var errorResponseException = innerException as ErrorResponseException;
                if (errorResponseException != null)
                    throw errorResponseException.SimplifyException();

                throw innerException;
            }).Unwrap();
        }

        ///<summary>
        /// Turn an expression like x=&lt; x.User.Name to "User.Name"
        ///</summary>
        public static string ToPropertyPath(this LambdaExpression expr,
            char propertySeparator = '.',
            char collectionSeparator = ',')
        {
            var expression = expr.Body;

            return expression.ToPropertyPath(propertySeparator, collectionSeparator);
        }

        public static string ToPropertyPath(this Expression expression, char propertySeparator = '.', char collectionSeparator = ',')
        {
            var propertyPathExpressionVisitor = new PropertyPathExpressionVisitor(propertySeparator.ToString(), collectionSeparator.ToString());
            propertyPathExpressionVisitor.Visit(expression);

            var builder = new StringBuilder();
            foreach (var result in propertyPathExpressionVisitor.Results)
            {
                builder.Append(result);
            }
            return builder.ToString().Trim(propertySeparator, collectionSeparator);
        }


        public static Exception SimplifyException(this Exception exception)
        {
            var aggregateException = exception as AggregateException;
            if (aggregateException != null)
            {
                var innerException = aggregateException.ExtractSingleInnerException();
                if (innerException != null)
                    return innerException.SimplifyException();
            }

            var errorResponseException = exception as ErrorResponseException;
            if (errorResponseException != null)
                return errorResponseException.SimplifyException();

            return exception;
        }

        /// <summary>
        ///     Extracts a portion of an exception for a user friendly display
        /// </summary>
        /// <param name="e">The exception.</param>
        /// <returns>The primary portion of the exception message.</returns>
        public static string SimplifyError(this Exception e)
        {
            var parts = e.Message.Split(new[] { "\r\n   " }, StringSplitOptions.None);
            var firstLine = parts.First();
            var index = firstLine.IndexOf(':');
            return index > 0
                       ? firstLine.Remove(0, index + 2)
                       : firstLine;
        }

        public class PropertyPathExpressionVisitor : ExpressionVisitor
        {
            private readonly string propertySeparator;
            private readonly string collectionSeparator;
            public Stack<string> Results = new Stack<string>();

            public PropertyPathExpressionVisitor(string propertySeparator, string collectionSeparator)
            {
                this.propertySeparator = propertySeparator;
                this.collectionSeparator = collectionSeparator;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                Results.Push(propertySeparator);
                Results.Push(node.Member.Name);
                return base.VisitMember(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.Name != "Select" && node.Arguments.Count != 2)
                    throw new InvalidOperationException("Not idea how to deal with convert " + node + " to a member expression");


                Visit(node.Arguments[1]);
                Results.Push(collectionSeparator);
                Visit(node.Arguments[0]);


                return node;
            }
        }
    }
}
