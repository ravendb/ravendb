using Raven.Database.Config;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			CertGenerator.GenerateNewCertificate("raven.cert");
		}
	}
}