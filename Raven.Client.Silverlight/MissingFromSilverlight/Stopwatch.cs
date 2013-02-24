namespace System.Diagnostics
{
	public class Stopwatch
	{
		public static Stopwatch StartNew()
		{
			var sp = new Stopwatch();
			sp.Start();
			return sp;
		}

		private long start;
		private long? end;

		private long End
		{
			get { return end ?? Environment.TickCount; }
		}

		public void Start()
		{
			start = Environment.TickCount;
		}

		public TimeSpan Elapsed
		{
			get
			{
				if(start == 0)
					return TimeSpan.Zero;
				return new TimeSpan(End - start);
			}
		}

		public long ElapsedMilliseconds
		{
			get { return End - start; }
		}

		public void Stop()
		{
			end = Environment.TickCount;
		}
	}
}