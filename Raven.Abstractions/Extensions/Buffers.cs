namespace Raven.Abstractions.Extensions
{
	public class Buffers
	{
		public static int Compare(byte[] x, byte[] y)
		{
			if (x.Length != y.Length)
				return x.Length - y.Length;

			for (int i = 0; i < x.Length; i++)
			{
				if (x[i] != y[i])
				{
					return x[i] - y[i];
				}
			}
			return 0;
		}
	}
}