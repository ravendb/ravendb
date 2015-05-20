using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http.Filters;
using System.Web.Http.Results;
using Jint.Runtime;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Controllers;
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
				{typeof (JavaScriptException), (ctx, e) => HandleJintException(ctx, e as JavaScriptException)},
				{typeof (IndexDisabledException), (ctx, e) => HandleIndexDisabledException(ctx, e as IndexDisabledException)},
				{typeof (IndexDoesNotExistsException), (ctx, e) => HandleIndexDoesNotExistsException(ctx, e as IndexDoesNotExistsException)},
                {typeof (ImplicitFetchFieldsFromDocumentNotAllowedException), (ctx, e) => HandleImplicitFetchFieldsFromDocumentNotAllowedException(ctx, e as ImplicitFetchFieldsFromDocumentNotAllowedException)},
				{typeof (SynchronizationException), (ctx, e) => HandleSynchronizationException(ctx, e as SynchronizationException)},
				{typeof (FileNotFoundException), (ctx, e) => HandleFileNotFoundException(ctx, e as FileNotFoundException)},
				{typeof (SubscriptionException), (ctx, e) => HandleSubscriptionException(ctx, e as SubscriptionException)}
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
			if (ctx.Request.Method == HttpMethods.Head) // head request must not return a message body in the response
				return;

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
			if (ctx.ActionContext.ControllerContext.Controller is RavenFsApiController)
			{
				ctx.Response = new HttpResponseMessage
				{
					StatusCode = HttpStatusCode.MethodNotAllowed,
				};
			}
			else 
			{
				ctx.Response = new HttpResponseMessage
				{
					StatusCode = HttpStatusCode.Conflict,
				};
			}

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				ActualETag = e.ActualETag ?? Etag.Empty,
				ExpectedETag = e.ExpectedETag ?? Etag.Empty,
				Error = e.Message
			});
		}

		private static void HandleJintException(HttpActionExecutedContext ctx, JavaScriptException e)
		{
			//while (e.InnerException is JintException)
			//{
			//	e = (JintException)e.InnerException;
			//}

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
				StatusCode = HttpStatusCode.InternalServerError,
			};

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				Error = e.Information == null ? e.ToString() : e.Information.GetErrorMessage(),
			});
		}

	    private static void HandleImplicitFetchFieldsFromDocumentNotAllowedException(HttpActionExecutedContext ctx, ImplicitFetchFieldsFromDocumentNotAllowedException e)
	    {
	        ctx.Response = new HttpResponseMessage
	        {
	            StatusCode = HttpStatusCode.InternalServerError
	        };

            SerializeError(ctx, new
            {
                Url = ctx.Request.RequestUri.PathAndQuery,
                Error = e.Message
            });
	    }


		private static void HandleSynchronizationException(HttpActionExecutedContext ctx, SynchronizationException e)
		{
			ctx.Response = new HttpResponseMessage
			{
				StatusCode = (HttpStatusCode) 420
			};

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				Error = e.Message
			});
		}

		private static void HandleFileNotFoundException(HttpActionExecutedContext ctx, FileNotFoundException e)
		{
			ctx.Response = new HttpResponseMessage
			{
				StatusCode = HttpStatusCode.NotFound
			};

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				Error = e.Message
			});
		}

		private static void HandleSubscriptionException(HttpActionExecutedContext ctx, SubscriptionException e)
		{
			ctx.Response = new HttpResponseMessage
			{
				StatusCode = e.ResponseStatusCode
			};

			SerializeError(ctx, new
			{
				Url = ctx.Request.RequestUri.PathAndQuery,
				Error = e.Message
			});
		}
	}
}
