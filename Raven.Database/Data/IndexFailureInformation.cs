namespace Raven.Database.Data
{
	public class IndexFailureInformation
	{
		public bool IsInvalidIndex
		{
			get
			{
				if (Attempts == 0 || Errors == 0)
					return false;
				// we don't have enough attempts to make a useful determination
				if (Attempts < 10)
					return false;
				return (Errors/(float) Attempts) > 0.15;
			}
		}

		public string Name { get; set; }
		public int Attempts { get; set; }
		public int Errors { get; set; }
		public int Successes { get; set; }

		public float FailureRate
		{
			get
			{
				if (Attempts == 0)
					return 0;
				return (Errors / (float)Attempts);
			}
		}

		public string GetErrorMessage()
		{
			const string msg =
				"Index {0} is invalid, out of {1} indexing attempts, {2} has failed.\r\nError rate of {3:#.##%} exceeds allowed 15% error rate";
			return string.Format(msg,
			                     Name, Attempts, Errors, FailureRate);
		}
	}
}