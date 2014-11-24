// -----------------------------------------------------------------------
//  <copyright file="OperationCredentials.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;

namespace Raven.Abstractions.Connection
{
	public class OperationCredentials
	{
		public OperationCredentials(string apiKey, ICredentials credentials)
		{
			ApiKey = apiKey;
			Credentials = credentials;
		}

		public ICredentials Credentials { get; private set; }

		public string ApiKey { get; private set; }

		public bool HasCredentials()
		{
			return !string.IsNullOrEmpty(ApiKey) || Credentials != null;
		}
	}
}