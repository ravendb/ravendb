using System;
using Nevar.Trees;

namespace Nevar.Impl
{
	public interface IVirtualPager : IDisposable
	{
		Page Get(long n);
	}
}