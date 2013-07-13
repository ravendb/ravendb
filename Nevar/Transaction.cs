using System.Collections.Generic;

namespace Nevar
{
	public class Transaction
	{
		public int NextPageNumber;
		public List<Page> DirtyPages = new List<Page>();

		public Pager Pager;

		public Page GetPage(int n)
		{
			return Pager.Get(n);
		}

		public Page AllocatePage(int num)
		{
			return Pager.Allocate(this, num);
		}
	}
}