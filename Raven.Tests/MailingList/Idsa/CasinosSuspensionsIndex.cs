using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Raven.Tests.MailingList.Idsa
{
	public class CasinosSuspensionsIndex : AbstractIndexCreationTask<Casino, CasinosSuspensionsIndex.IndexResult>
	{
		public class IndexResult
		{
			public string CityId { get; set; }
			public string CasinoId { get; set; }
			public string CasinoAddress { get; set; }
			public string Id { get; set; }
			public DateTime DateTime { get; set; }
			public IList<Exemption> Exemptions { get; set; }
		}

		public CasinosSuspensionsIndex()
		{
			Map = casinos => from casino in casinos
							 from suspension in casino.Suspensions
							 select new
							 {
								 CityId = casino.CityId,
								 CasinoId = casino.Id,
								 CasinoAddress = casino.Address,
								 Id = suspension.Id,
								 DateTime = suspension.DateTime,
								 Exemptions = (object[])suspension.Exemptions ?? new object[0]
							 };

			Store(x => x.CityId, FieldStorage.Yes);
			Store(x => x.CasinoId, FieldStorage.Yes);
			Store(x => x.CasinoAddress, FieldStorage.Yes);
			Store(x => x.Id, FieldStorage.Yes);
			Store(x => x.DateTime, FieldStorage.Yes);
			Store(x => x.Exemptions, FieldStorage.Yes);
		}
	}
}