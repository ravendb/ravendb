using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http.Filters;
using Jint;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database.Server.Controllers;
using Raven.Json.Linq;

namespace Raven.Database.Server.WebApi.Filters
{
	public class RavenExceptionFilterAttribute : ExceptionFilterAttribute
	{
		private static readonly Dictionary<Type, Action<HttpActionExecutedContext, Exception>> handlers =
				new Dictionary<Type, Action<HttpActionExecutedContext, Exception>>
			{
				{typeof (BadRequestException), (ctx, e) => HandleBadRequest(ctx, e as BadRequestException)},
				{typeof (ConcurrencyException), (ctx, e) => HandleConcurrencyException(ctx, e as ConcurrencyException)},
				{typeof (JintException), (ctx, e) => HandleJintException(ctx, e as JintException)},
				{typeof (IndexDisabledException), (ctx, e) => HandleIndexDisabledException(ctx, e as IndexDisabledException)},
				{typeof (IndexDoesNotExistsException), (ctx, e) => HandleIndexDoesNotExistsException(ctx, e as IndexDoesNotExistsException)},
			};

		public override void OnException(HttpActionExecutedContext ctx)
		{
			var e = ctx.Exception;
			var exceptionType = e.GetType();

			try
			{
				if (handlers.ContainsKey(exceptionType))
				{
					handlers[exceptionType](ctx, e);
					return;
				}

				var baseType = handlers.Keys.FirstOrDefault(t => t.IsInstanceOfType(e));
				if (baseType != null)
				{
					handlers[baseType](ctx, e);
					return;
				}

				DefaultHandler(ctx, e);
			}
			catch (Exception)
			{
				//TODO: log
				//logger.ErrorException("Failed to properly handle error, further error handling is ignored", e);
			}
		}

		public static void SerializeError(HttpActionExecutedContext ctx, object error)
		{
			ctx.Response.Content = new JsonContent(RavenJObject.FromObject(error))
				.WithRequest(ctx.Request);
		}

		private static void DefaultHandler(HttpActionExecutedContext ctx, Exception e)
		{
			ctx.Response = new HttpResponseMessage
			{
				StatusCode = HttpStatusCode.InternalServerError,
			};

			SerializeError(ctx, new
			{
				//ExceptionType = e.GetType().AssemblyQualifiedName,					
				Url = ctx.Request.RequestUri.PathAndQuery,
				Error = e.ToString(),
			});
		}

		private static void HandleBadRequest(HttpActionExecutedContext ctx, BadRequestException e)
		{
			ctx.Response = new HttpResponseMessage
			{
				StatusCode = HttpStatusCode.BadRequest,
			};

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				e.Message,
				Error = e.Message
			});
		}

		private static void HandleConcurrencyException(HttpActionExecutedContext ctx, ConcurrencyException e)
		{
			ctx.Response = new HttpResponseMessage
			{
				StatusCode = HttpStatusCode.Conflict,
			};

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				ActualETag = e.ActualETag ?? Etag.Empty,
				ExpectedETag = e.ExpectedETag ?? Etag.Empty,
				Error = e.Message
			});
		}

		private static void HandleJintException(HttpActionExecutedContext ctx, JintException e)
		{
			while (e.InnerException is JintException)
			{
				e = (JintException)e.InnerException;
			}

			ctx.Response = new HttpResponseMessage
			{
				StatusCode = HttpStatusCode.BadRequest,
			};

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				Error = e.Message
			});
		}

		private static void HandleIndexDoesNotExistsException(HttpActionExecutedContext ctx, IndexDoesNotExistsException e)
		{
			ctx.Response = new HttpResponseMessage
			{
				StatusCode = HttpStatusCode.NotFound,
			};

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				Error = e.Message
			});
		}

		private static void HandleIndexDisabledException(HttpActionExecutedContext ctx, IndexDisabledException e)
		{
			ctx.Response = new HttpResponseMessage
			{
				StatusCode = HttpStatusCode.ServiceUnavailable,
			};

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				Error = e.Information == null ? e.Message : e.Information.GetErrorMessage(),
			});
		}
	}
}
