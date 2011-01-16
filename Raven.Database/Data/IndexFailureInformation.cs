//-----------------------------------------------------------------------
// <copyright file="IndexFailureInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Database.Data
{
	/// <summary>
	/// Information about index failure rates
	/// </summary>
	public class IndexFailureInformation
	{
		/// <summary>
		/// Gets a value indicating whether this is invalid index.
		/// </summary>
		/// <value>
		/// 	<c>true</c> if this is invalid index; otherwise, <c>false</c>.
		/// </value>
		public bool IsInvalidIndex
		{
			get
			{
				if (Attempts == 0 || Errors == 0)
					return false;
				// we don't have enough attempts to make a useful determination
				if (Attempts < 100)
					return false;
				return (Errors/(float) Attempts) > 0.15;
			}
		}

		/// <summary>
		/// Gets or sets the name.
		/// </summary>
		/// <value>The name.</value>
		public string Name { get; set; }
		/// <summary>
		/// Gets or sets the number of indexing attempts.
		/// </summary>
		public int Attempts { get; set; }
		/// <summary>
		/// Gets or sets the number of indexing errors.
		/// </summary>
		public int Errors { get; set; }
		/// <summary>
		/// Gets or sets the number of indexing successes.
		/// </summary>
		public int Successes { get; set; }

		/// <summary>
		/// Gets the failure rate.
		/// </summary>
		/// <value>The failure rate.</value>
		public float FailureRate
		{
			get
			{
				if (Attempts == 0)
					return 0;
				return (Errors / (float)Attempts);
			}
		}

		/// <summary>
		/// Gets the error message.
		/// </summary>
		/// <returns></returns>
		public string GetErrorMessage()
		{
			const string msg =
				"Index {0} is invalid, out of {1} indexing attempts, {2} has failed.\r\nError rate of {3:#.##%} exceeds allowed 15% error rate";
			return string.Format(msg,
			                     Name, Attempts, Errors, FailureRate);
		}
	}
}
