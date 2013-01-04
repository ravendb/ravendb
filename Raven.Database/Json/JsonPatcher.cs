//-----------------------------------------------------------------------
// <copyright file="JsonPatcher.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Json;
using System.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Json
{
	public class JsonPatcher
	{
		private readonly RavenJObject document;

		public JsonPatcher(RavenJObject document)
		{
			this.document = document;
		}

		public RavenJObject Apply(PatchRequest[] patch)
		{
			foreach (var patchCmd in patch)
			{
				Apply(patchCmd);
			}
			return document;
		}

		private void Apply(PatchRequest patchCmd)
		{
			if (patchCmd.Name == null)
				throw new InvalidOperationException("Patch property must have a name property");
			foreach (var result in document.SelectTokenWithRavenSyntaxReturningFlatStructure( patchCmd.Name ))
			{
			    var token = result.Item1;
			    var parent = result.Item2;
				switch (patchCmd.Type)
				{
					case PatchCommandType.Set:
						SetProperty(patchCmd, patchCmd.Name, token);
						break;
					case PatchCommandType.Unset:
						RemoveProperty(patchCmd, patchCmd.Name, token, parent);
						break;
					case PatchCommandType.Add:
						AddValue(patchCmd, patchCmd.Name, token);
						break;
					case PatchCommandType.Insert:
						InsertValue(patchCmd, patchCmd.Name, token);
						break;
					case PatchCommandType.Remove:
						RemoveValue(patchCmd, patchCmd.Name, token);
						break;
					case PatchCommandType.Modify:
						ModifyValue(patchCmd, patchCmd.Name, token);
						break;
					case PatchCommandType.Inc:
						IncrementProperty(patchCmd, patchCmd.Name, token);
						break;
					case PatchCommandType.Copy:
						CopyProperty(patchCmd, token);
						break;
					case PatchCommandType.Rename:
						RenameProperty(patchCmd, patchCmd.Name, token);
						break;
					default:
						throw new ArgumentException(string.Format("Cannot understand command: '{0}'", patchCmd.Type));
				}
			}
		}

		private void RenameProperty(PatchRequest patchCmd, string propName, RavenJToken property)
		{
			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
			if (property == null)
				return;

			document[patchCmd.Value.Value<string>()] = property;
			document.Remove(propName);
		}

		private void CopyProperty(PatchRequest patchCmd, RavenJToken property)
		{
			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
			if (property == null)
				return;

			document[patchCmd.Value.Value<string>()] = property;
		}

		private static void ModifyValue(PatchRequest patchCmd, string propName, RavenJToken property)
		{
			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
			if (property == null)
				throw new InvalidOperationException("Cannot modify value from '" + propName + "' because it was not found");

			var nestedCommands = patchCmd.Nested;
			if (nestedCommands == null)
				throw new InvalidOperationException("Cannot understand modified value from '" + propName +
													"' because could not find nested array of commands");

			var arrayOrValue = TryGetArray(property) ?? property;
			switch (arrayOrValue.Type)
			{
				case JTokenType.Object:
					foreach (var cmd in nestedCommands)
					{
						var nestedDoc = property.Value<RavenJObject>();
						new JsonPatcher(nestedDoc).Apply(cmd);
					}
					break;
				case JTokenType.Array:
					var position = patchCmd.Position;
					var allPositionsIsSelected = patchCmd.AllPositions.HasValue ? patchCmd.AllPositions.Value : false;
					if (position == null && !allPositionsIsSelected)
						throw new InvalidOperationException("Cannot modify value from  '" + propName +
						                                    "' because position element does not exists or not an integer and allPositions is not set");
					var valueList = new List<RavenJToken>();
					if (allPositionsIsSelected)
					{
						valueList.AddRange(arrayOrValue.Values<RavenJToken>());
					}
					else
					{
						valueList.Add(((RavenJArray)arrayOrValue)[position.Value]);
					}

					foreach (var value in valueList)
					{
						foreach (var cmd in nestedCommands)
						{
							new JsonPatcher(value.Value<RavenJObject>()).Apply(cmd);
						}
					}
					break;
				default:
					throw new InvalidOperationException("Can't understand how to deal with: " + property.Type);
			}
		}

		private void RemoveValue(PatchRequest patchCmd, string propName, RavenJToken token)
		{
			EnsurePreviousValueMatchCurrentValue(patchCmd, token);
			if (token == null)
			{
				token = new RavenJArray();
				document[propName] = token;
			}
			var array = GetArray(token, propName);

			var position = patchCmd.Position;
			var value = patchCmd.Value;
			if (position == null && (value == null || value.Type == JTokenType.Null))
				throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because position element does not exists or not an integer and no value was present");
			if (position != null && value != null && value.Type != JTokenType.Null)
				throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because both a position and a value are set");
			if (position != null && (position.Value < 0 || position.Value >= array.Length))
				throw new IndexOutOfRangeException("Cannot remove value from  '" + propName +
												   "' because position element is out of bound bounds");

			if (value != null && value.Type != JTokenType.Null)
			{
				foreach (var ravenJToken in array.Where(x => RavenJToken.DeepEquals(x, value)).ToList())
				{
					array.Remove(ravenJToken);
				}

				return;
			}

			if(position!=null)
				array.RemoveAt(position.Value);
		}

		private void InsertValue(PatchRequest patchCmd, string propName, RavenJToken property)
		{
			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
			if (!(property is RavenJArray))
			{
				property = new RavenJArray();
				document[propName] = property;
			}
			var array = property as RavenJArray;
			if (array == null)
				throw new InvalidOperationException("Cannot remove value from '" + propName + "' because it is not an array");
			var position = patchCmd.Position;
			if (position == null)
				throw new InvalidOperationException("Cannot remove value from '" + propName + "' because position element does not exists or not an integer");
			if (position < 0 || position >= array.Length)
				throw new IndexOutOfRangeException("Cannot remove value from '" + propName +
												   "' because position element is out of bound bounds");
			array.Insert(position.Value, patchCmd.Value);
		}

		private void AddValue(PatchRequest patchCmd, string propName, RavenJToken token)
		{
			EnsurePreviousValueMatchCurrentValue(patchCmd, token);
			if (token == null)
			{
				token = new RavenJArray();
				document[propName] = token;
			}
			var array = GetArray(token, propName);

			array.Add(patchCmd.Value);
		}

		private static RavenJArray GetArray(RavenJToken property, string propName)
		{
			var array = TryGetArray(property);
			if(array == null)
				throw new InvalidOperationException("Cannot modify '" + propName + "' because it is not an array");
			return array;
		}

		private static RavenJArray TryGetArray(RavenJToken token)
		{
			var array = token as RavenJArray;
			if (array != null)
				return array;

			var jObject = token as RavenJObject;
			if (jObject == null || !jObject.ContainsKey("$values"))
				return null;
			array = jObject.Value<RavenJArray>("$values");

			return array;
		}


		private static void RemoveProperty(PatchRequest patchCmd, string propName, RavenJToken token, RavenJToken parent)
		{
			EnsurePreviousValueMatchCurrentValue(patchCmd, token);
			var o = parent as RavenJObject;
			if (o != null)
				o.Remove(propName);
		}

		private void SetProperty(PatchRequest patchCmd, string propName, RavenJToken property)
		{
			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
			document[propName] = patchCmd.Value;
		}


		private void IncrementProperty(PatchRequest patchCmd, string propName, RavenJToken property)
		{
			if (patchCmd.Value.Type != JTokenType.Integer)
				throw new InvalidOperationException("Cannot increment when value is not an integer");

			var valToSet = patchCmd.Value as RavenJValue; // never null since we made sure it's JTokenType.Integer

			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
			var val = property as RavenJValue;
			if (val == null)
			{
				document[propName] = valToSet.Value<int>();
				return;
			}
			if (val.Value == null || val.Type == JTokenType.Null)
				val.Value = valToSet.Value<int>();
			else
				val.Value = RavenJToken.FromObject(val.Value<int>() + valToSet.Value<int>()).Value<int>();
		}

		private static void EnsurePreviousValueMatchCurrentValue(PatchRequest patchCmd, RavenJToken property)
		{
			var prevVal = patchCmd.PrevVal;
			if (prevVal == null)
				return;
			switch (prevVal.Type)
			{
				case JTokenType.Undefined:
					if (property != null)
						throw new ConcurrencyException();
					break;
				default:
					if (property == null)
						throw new ConcurrencyException();
					if (RavenJToken.DeepEquals(property, prevVal) == false)
						throw new ConcurrencyException();
					break;
			}
		}
	}
}
