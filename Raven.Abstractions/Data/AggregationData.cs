using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
	public class AggregationData
	{
		public string AggregateOn { get; set; }
		public string CalculateOn { get; set; }

		public bool HasData()
		{
			if (string.IsNullOrWhiteSpace(AggregateOn) || string.IsNullOrWhiteSpace(CalculateOn))
				return false;

			return Max || Min || Count || Sum || Average;
		}

		[JsonIgnore]
		public FacetAggregation FacetAggregation
		{
			get
			{
				var result = FacetAggregation.None;

				if (Max)
					result |= FacetAggregation.Max;
				if (Min)
					result |= FacetAggregation.Min;
				if (Average)
					result |= FacetAggregation.Average;
				if (Count)
					result |= FacetAggregation.Count;
				if (Sum)
					result |= FacetAggregation.Sum;

				return result;
			}
		}

		public bool Max { get; set; }
		public bool Min { get; set; }
		public bool Count { get; set; }
		public bool Average { get; set; }
		public bool Sum { get; set; }
	}
}
