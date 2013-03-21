//-----------------------------------------------------------------------
// <copyright file="ReadVetoResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Database.Plugins
{
	public class ReadVetoResult
	{
		public static ReadVetoResult Allowed
		{
			get { return new ReadVetoResult(ReadAllow.Allow, "allowed"); }
		}

		public static ReadVetoResult Ignore
		{
			get { return new ReadVetoResult(ReadAllow.Ignore, "ignore"); }
		}

		public static ReadVetoResult Deny(string reason)
		{
			return new ReadVetoResult(ReadAllow.Deny, reason);
		}

		private ReadVetoResult(ReadAllow allowed, string reason)
		{
			Veto = allowed;
			Reason = reason;
		}

		public ReadAllow Veto { get; private set; }

		public enum ReadAllow
		{
			Allow,
			Deny,
			Ignore
		}

		public string Reason { get; private set; }
	}
}
