using System;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	public class IdentityController : RavenApiController
	{
		[HttpPost][Route("identity/next")]
		public HttpResponseMessage IdentityNextPost()
		{
			var name = GetQueryStringValue("name");
			if (string.IsNullOrWhiteSpace(name))
			{
				return GetMessageWithObject(new
				{
					Error = "'name' query string parameter is mandatory and cannot be empty"
				}, HttpStatusCode.BadRequest);
			}

			long nextIdentityValue = -1;
			Database.TransactionalStorage.Batch(accessor =>
			{
				nextIdentityValue = accessor.General.GetNextIdentityValue(name);
			});

			return GetMessageWithObject(new { Value = nextIdentityValue });
		}

		[HttpPost][Route("identity/seed")]
		public HttpResponseMessage IdentitySeed()
		{
			var name = GetQueryStringValue("name");
			var valueString = GetQueryStringValue("value");

			if (string.IsNullOrWhiteSpace(name))
			{
				return GetMessageWithObject(new
				{
					Error = "'name' query string parameter is mandatory and cannot be empty"
				}, HttpStatusCode.BadRequest);
			}

			if (string.IsNullOrWhiteSpace(valueString))
			{
				return GetMessageWithObject(new
				{
					Error = "'seed' query string parameter is mandatory and cannot be empty"
				}, HttpStatusCode.BadRequest);
			}

			long value;
			if (!Int64.TryParse(valueString, out value))
			{
				return GetMessageWithObject(new
				{
					Error = "'seed' query string parameter must be an integer"
				}, HttpStatusCode.BadRequest);
			}

			Database.TransactionalStorage.Batch(accessor => accessor.General.SetIdentityValue(name, value));
			return GetMessageWithObject(new
			{
				Value = value
			});
		}
	}
}