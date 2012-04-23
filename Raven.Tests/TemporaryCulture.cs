using System;
using System.Globalization;
using System.Threading;

namespace Raven.Tests
{
	public class TemporaryCulture: IDisposable
	{
		private readonly CultureInfo oldCurrentCulture;
		private readonly CultureInfo oldCurrentUiCulture;

		public TemporaryCulture(CultureInfo cultureInfo)
		{
			oldCurrentCulture = Thread.CurrentThread.CurrentCulture;
			oldCurrentUiCulture = Thread.CurrentThread.CurrentUICulture;

			Thread.CurrentThread.CurrentCulture = cultureInfo;
			Thread.CurrentThread.CurrentUICulture = cultureInfo;
		}

		public TemporaryCulture(string cultureName)
			:this(new CultureInfo(cultureName))
		{
		}

		public void Dispose()
		{
			Thread.CurrentThread.CurrentCulture = oldCurrentCulture;
			Thread.CurrentThread.CurrentUICulture = oldCurrentUiCulture;
		}
	}
}