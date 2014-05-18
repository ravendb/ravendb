using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.RavenFS
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

				var webException = task.Exception.ExtractSingleInnerException() as WebException;
				if (webException == null || webException.Response == null)
					return task;

				throw webException.BetterWebExceptionError();
			})
					   .Unwrap();
		}
		public static Exception BetterWebExceptionError(this ErrorResponseException webException)
		{
			if (webException.StatusCode == (HttpStatusCode)420)
			{
				return
					new JsonSerializer().Deserialize<SynchronizationException>(
						new JsonTextReader(new StringReader(webException.Message)));
			}
			if (webException.StatusCode == HttpStatusCode.MethodNotAllowed)
			{
				return new JsonSerializer().Deserialize<ConcurrencyException>(new JsonTextReader(new StringReader(webException.Message)));
			}
			if (webException.StatusCode == HttpStatusCode.NotFound)
			{
				return new FileNotFoundException();
			}

			using (var reader = new StringReader(webException.Message))
			{
				var readToEnd = reader.ReadToEnd();
				return new InvalidOperationException(
					webException + Environment.NewLine + readToEnd, webException);
			}
		}

		public static Exception BetterWebExceptionError(this WebException webException)
		{
			var httpWebResponse = webException.Response as HttpWebResponse;
			if (httpWebResponse != null)
			{
				if (httpWebResponse.StatusCode == (HttpStatusCode)420)
				{
					using (var stream = webException.Response.GetResponseStream())
					{
						return new JsonSerializer().Deserialize<SynchronizationException>(new JsonTextReader(new StreamReader(stream)));
					}
				}
				else if (httpWebResponse.StatusCode == HttpStatusCode.MethodNotAllowed)
				{
					using (var stream = webException.Response.GetResponseStream())
					{
						return new JsonSerializer().Deserialize<ConcurrencyException>(new JsonTextReader(new StreamReader(stream)));
					}
				}
				else if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
				{
					return new FileNotFoundException();
				}
			}

			using (var stream = webException.Response.GetResponseStream())
			using (var reader = new StreamReader(stream))
			{
				var readToEnd = reader.ReadToEnd();
				return new InvalidOperationException(
					webException + Environment.NewLine + readToEnd);
			}
		}

		public static Task<T> TryThrowBetterError<T>(this Task<T> self)
		{
			return self.ContinueWith(task =>
			{
				if (task.Status != TaskStatus.Faulted)
					return task;

				var webException = task.Exception.ExtractSingleInnerException() as WebException;
				if (webException == null || webException.Response == null)
					return task;

				throw webException.BetterWebExceptionError();
			})
					   .Unwrap();
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
#if NETFX_CORE
			var propertyPathExpressionVisitor = new PropertyPathExpressionVisitor(propertySeparator.ToString(), collectionSeparator.ToString());
#else
			var propertyPathExpressionVisitor = new PropertyPathExpressionVisitor(propertySeparator.ToString(CultureInfo.InvariantCulture), collectionSeparator.ToString(CultureInfo.InvariantCulture));
#endif
			propertyPathExpressionVisitor.Visit(expression);

			var builder = new StringBuilder();
			foreach (var result in propertyPathExpressionVisitor.Results)
			{
				builder.Append(result);
			}
			return builder.ToString().Trim(propertySeparator, collectionSeparator);
		}


		public static Exception TryThrowBetterError(this Exception exception)
		{
			var httpException = exception as ErrorResponseException;
			if (httpException != null)
			{
				return httpException.BetterWebExceptionError();
			}
			var aggregateException = exception as AggregateException;
			if (aggregateException == null)
			{
				var web = exception as WebException;
				return web != null ? web.BetterWebExceptionError() : exception;
			}
			var webException = aggregateException.ExtractSingleInnerException() as WebException;
			if (webException == null || webException.Response == null)
				return aggregateException;

			return webException.BetterWebExceptionError();
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
