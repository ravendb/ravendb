using System.Collections.Generic;
using System.Linq;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Studio.Commands;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class SpatialQueryField
	{
		public string Name { get; set; }
		public bool IsGeographical { get; set; }
		public SpatialUnits Units { get; set; }
	}

	public class SpatialQueryModel : NotifyPropertyChangedBase
	{
		protected PerDatabaseState PerDatabaseState
		{
			get { return ApplicationModel.Current.State.Databases[ApplicationModel.Database.Value]; }
		}

		public SpatialQueryModel()
		{
			Fields = new BindableCollection<string>(x => x);
		}

		public string IndexName { get; set; }
		private readonly Dictionary<string, SpatialQueryField> fields = new Dictionary<string, SpatialQueryField>();
		public BindableCollection<string> Fields { get; private set; }

		public void Clear()
		{
			Y = null;
			X = null;
			Radius = null;
		}

		public void Reset()
		{
			FieldName = Fields.FirstOrDefault();
			Clear();
		}

		public void UpdateFields(IEnumerable<SpatialQueryField> queryFields)
		{
			fields.Clear();
			Fields.Clear();
			
			foreach (var queryField in queryFields.OrderBy(c => c.Name))
			{
				Fields.Add(queryField.Name);
				fields[queryField.Name] = queryField;
			}

			FieldName = Fields.FirstOrDefault();
		}

		public void UpdateFromState(QueryState state)
		{
			if (state.IsSpatialQuery)
			{
				var key = state.SpatialFieldName ?? Constants.DefaultSpatialFieldName;

				if (!fields.ContainsKey(key))
					key = Fields.FirstOrDefault();

				FieldName = key;

				Y = state.Latitude;
				X = state.Longitude;
				Radius = state.Radius;
			}
			else
			{
				Reset();
			}
		}

		private SpatialUnits radiusUnits;
		public SpatialUnits RadiusUnits
		{
			get { return radiusUnits; }
			set
			{
				if (radiusUnits == value) return;
				radiusUnits = value;
				OnPropertyChanged(() => RadiusUnits);
			}
		}

		private string fieldName;
		public string FieldName
		{
			get { return fieldName; }
			set
			{
				if (fieldName == value) return;
				fieldName = value;
				if (!string.IsNullOrWhiteSpace(fieldName))
				{
					IsGeographical = fields[fieldName].IsGeographical;
					RadiusUnits = fields[fieldName].Units;
				}
				else
				{
					IsGeographical = true;
					RadiusUnits = SpatialUnits.Kilometers;
				}
				OnPropertyChanged(() => FieldName);
			}
		}

		private bool isGeographical;
		public bool IsGeographical
		{
			get { return isGeographical; }
			set
			{
				if (isGeographical == value) return;
				isGeographical = value;
				OnPropertyChanged(() => IsGeographical);
			}
		}

		private double? x;
		public double? X
		{
			get { return x; }
			set
			{
				if (x == value) return;
				x = value;
				address = null;
				OnPropertyChanged(() => Address);
				OnPropertyChanged(() => X);
			}
		}

		private double? y;
		public double? Y
		{
			get { return y; }
			set
			{
				if (y == value) return;
				y = value;
				address = null;
				OnPropertyChanged(() => Address);
				OnPropertyChanged(() => Y);
			}
		}

		private double? radius;
		public double? Radius
		{
			get { return radius; }
			set
			{
				if (radius == value) return;
				radius = value;
				OnPropertyChanged(() => Radius);
			}
		}

		private string address;
		public string Address
		{
			get { return address; }
			set
			{
				UpdateAddress(value);
				OnPropertyChanged(() => X);
				OnPropertyChanged(() => Y);
				OnPropertyChanged(() => Address);
			}
		}

		private void UpdateAddress(string value)
		{
			if (PerDatabaseState.RecentAddresses.ContainsKey(IndexName) && PerDatabaseState.RecentAddresses[IndexName].ContainsKey(value))
			{
				var data = PerDatabaseState.RecentAddresses[IndexName][value];
				if (data != null)
				{
					address = data.Address;
					y = data.Latitude;
					x = data.Longitude;
					return;
				}
			}

			address = value;
			y = null;
			x = null;
		}

		public void UpdateResultsFromCalculate(AddressData addressData)
		{
			y = addressData.Latitude;
			x = addressData.Longitude;

			var addresses = PerDatabaseState.RecentAddresses.ContainsKey(IndexName) ? PerDatabaseState.RecentAddresses[IndexName] : new Dictionary<string, AddressData>();

			addresses[addressData.Address] = addressData;

			PerDatabaseState.RecentAddresses[IndexName] = addresses;

			OnPropertyChanged(() => Y);
			OnPropertyChanged(() => X);
		}

		public ICommand CalculateFromAddress { get { return new CalculateGeocodeFromAddressCommand(this); } }
	}
}