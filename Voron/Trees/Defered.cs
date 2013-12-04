using System;
using System.Collections.Generic;

namespace Voron.Trees
{
	public class DeferedCusor
	{
		private Cursor _value;
		private readonly Func<Cursor> _create;
		private LinkedList<Action<Cursor>> _mutators;

		public DeferedCusor(Cursor value)
		{
			_value = value;
		}

		public DeferedCusor(Func<Cursor> create)
		{
			_create = create;
		}

		public Cursor Value
		{
			get
			{
				if (_value == null)
					CreateValue();
				return _value;
			}
		}

		private void CreateValue()
		{
			_value = _create();
			if (_mutators == null)
				return;
			foreach (var mutator in _mutators)
			{
				mutator(_value);
			}
		}

		public void Mutate(Action<Cursor> action)
		{
			if (_value != null)
			{
				action(_value);
				return;
			}

			if (_mutators == null)
				_mutators = new LinkedList<Action<Cursor>>();
			_mutators.AddLast(action);
		}
	}
}