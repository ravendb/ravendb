using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;

namespace Raven.Server.ServerWide
{
    public class ClusterChanges
    {
        public event Action<CompareExchangeChange> OnCompareExchangeChange;

        public delegate Task DatabaseChangedDelegate(string databaseName, long index, string type, DatabasesLandlord.ClusterDatabaseChangeType changeType, object changeState);

        public delegate Task ValueChangedDelegate(long index, string type);

        private DatabaseChangedDelegate[] _onDatabaseChanged = Array.Empty<DatabaseChangedDelegate>();
        private ValueChangedDelegate[] _onValueChanged = Array.Empty<ValueChangedDelegate>();

        public void RaiseNotifications(CompareExchangeChange compareExchangeChange)
        {
            OnCompareExchangeChange?.Invoke(compareExchangeChange);
        }

        public event DatabaseChangedDelegate DatabaseChanged
        {
            add
            {
                _onDatabaseChanged = _onDatabaseChanged.Concat(new[] { value }).ToArray();
            }
            remove
            {
                _onDatabaseChanged = _onDatabaseChanged.Where(x => x != value).ToArray();
            }
        }

        public event ValueChangedDelegate ValueChanged
        {
            add
            {
                _onValueChanged = _onValueChanged.Concat(new[] { value }).ToArray();
            }
            remove
            {
                _onValueChanged = _onValueChanged.Where(x => x != value).ToArray();
            }
        }

        public async Task OnDatabaseChanges(string databaseName, long index, string type, DatabasesLandlord.ClusterDatabaseChangeType changeType, object changeState)
        {
            var changes = _onDatabaseChanged;
            foreach (var act in changes)
            {
                await act(databaseName, index, type, changeType, changeState);
            }
        }

        public async Task OnValueChanges(long index, string type)
        {
            var changes = _onValueChanged;
            foreach (var act in changes)
            {
                await act(index, type);
            }
        }
    }
}
