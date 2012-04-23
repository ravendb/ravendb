using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using System.Collections;

namespace Raven.Abstractions.Json
{
	/// <summary>
	/// Convert a MultiDimensional Array to a json string
	/// </summary>
	public class JsonMultiDimensionalArrayConverter : JsonConverter
	{
		/// <summary>
		/// Determines whether this instance can convert the specified object type.
		/// </summary>
		/// <param name="objectType">Type of the object.</param>
		/// <returns>
		/// 	<c>true</c> if this instance can convert the specified object type; otherwise, <c>false</c>.
		/// </returns>
		public override bool CanConvert(Type objectType)
		{
			return objectType.IsArray && objectType.GetArrayRank() > 1;
		}

		/// <summary>
		/// Reads the JSON representation of the object.
		/// </summary>
		/// <param name="reader">The <see cref="T:Raven.Imports.Newtonsoft.Json.JsonReader"/> to read from.</param>
		/// <param name="objectType">Type of the object.</param>
		/// <param name="existingValue">The existing value of object being read.</param>
		/// <param name="serializer">The calling serializer.</param>
		/// <returns>The object value.</returns>
		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			// Handle null values
			if (reader.TokenType == JsonToken.Null) return null;

			var arrayItemType = objectType.GetElementType();
			var arrayRank = objectType.GetArrayRank();

			// Retrieve all the values from the Json
			var arrayValues = ReadRank(reader, serializer);

			// Determine the lengths of all ranks for the array
			var rankLengthList = GetRankLengthList(arrayValues);

			// If empty values were found, make sure the ranks match in size
			for (var i = rankLengthList.Count; i < arrayRank; i++)
			{
				rankLengthList.Add(0);
			}

			var rankLengthArray = rankLengthList.ToArray();

			// Create the array that will hold the values
			var retVal = Array.CreateInstance(arrayItemType, rankLengthArray);

			// Make the assignments
			SetValues(retVal, rankLengthArray, new int[0], 0, arrayValues);

			return retVal;
		}

		/// <summary>
		/// Writes the JSON representation of the object.
		/// </summary>
		/// <param name="writer">The <see cref="T:Raven.Imports.Newtonsoft.Json.JsonWriter"/> to write to.</param>
		/// <param name="value">The value.</param>
		/// <param name="serializer">The calling serializer.</param>
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			// Write out the array to Json
			WriteRank(writer, serializer, value as Array, 0, new int[0]);
		}


		#region Helpers
		/// <summary>
		/// Read in all the values from the Json reader and populate a nested ArrayList
		/// </summary>
		/// <param name="reader">JsonReader to use</param>
		/// <param name="serializer">JsonSerializer to use</param>
		/// <returns></returns>
		private List<object> ReadRank(JsonReader reader, JsonSerializer serializer)
		{
			var retVal = new List<object>();

			reader.Read();

			while (reader.TokenType != JsonToken.EndArray && reader.TokenType != JsonToken.Null && reader.TokenType != JsonToken.EndObject)
			{
				// If another array is found, it is a new rank
				// Otherwise, we have a value
				if (reader.TokenType == JsonToken.StartArray)
					retVal.Add(ReadRank(reader, serializer));
				else
					retVal.Add(reader.Value);

				reader.Read();
			}

			return retVal;
		}

		/// <summary>
		/// Retrieve a list of lengths for each rank represented
		/// </summary>
		/// <param name="arrayList">The list to process</param>
		/// <returns></returns>
		private List<int> GetRankLengthList(List<object> arrayList)
		{
			var retVal = new List<int>();

			retVal.Add(arrayList.Count);
			if (arrayList.Count > 0)
			{
				var childArrayList = arrayList[0] as List<object>;
				// If there are more children arrays, there are more ranks
				// Retrieve them and add to results
				if (childArrayList != null && childArrayList.Count > 0)
					retVal.AddRange(GetRankLengthList(childArrayList));
			}

			return retVal;
		}

		/// <summary>
		/// Assign values from the ArrayList into their respective place in the multidimensional array
		/// </summary>
		/// <param name="multiDimensionalArray">Array that will be receiving the newValues</param>
		/// <param name="rankLengthList">A list of the lengths of each rank</param>
		/// <param name="assignToIndexList">A list of the current index settings to be used for assignments</param>
		/// <param name="currentRank">Rank currently being processed</param>
		/// <param name="newValues">New Values that will be used in the assignment</param>
		private void SetValues(Array multiDimensionalArray, int[] rankLengthList, int[] assignToIndexList, int currentRank, List<object> newValues)
		{
			// Lengthen the assignToIndex and persist the values.
			var myAssignToIndex = assignToIndexList.GetUpperBound(0) + 1;
			var myAssignToIndexList = new int[myAssignToIndex + 1];
			Array.Copy(assignToIndexList, myAssignToIndexList, assignToIndexList.Length);

			// Loop through all values in the current rank for processing
			var currentRankLength = rankLengthList[currentRank];
			for (var i = 0; i < currentRankLength; i++)
			{
				// Assign currentIndex to the list of assignMentIndexes
				myAssignToIndexList[myAssignToIndex] = i;

				// If more ranks are found, process them
				// Otherwise, we have values, so make the assignment
				if (currentRank < multiDimensionalArray.Rank - 1)
					SetValues(multiDimensionalArray, rankLengthList, myAssignToIndexList, currentRank + 1, newValues[i] as List<object>);
				else
					multiDimensionalArray.SetValue(newValues[i], myAssignToIndexList);
			}
		}

		/// <summary>
		/// Write a rank of an array in Json format
		/// </summary>
		/// <param name="writer">JsonWriter in use</param>
		/// <param name="serializer">JsonSerializer in use</param>
		/// <param name="array">Array to be written</param>
		/// <param name="currentRank">Current rank "depth"</param>
		/// <param name="assignFromIndexList">List of indexes currently being used to read from the array</param>
		private void WriteRank(JsonWriter writer, JsonSerializer serializer, Array array, int currentRank, int[] assignFromIndexList)
		{
			writer.WriteStartArray();

			var lb = array.GetLowerBound(currentRank);
			var ub = array.GetUpperBound(currentRank);

			// Create a new indices list (one bigger than passed in) and fill with existing values
			var myAssignFromIndex = assignFromIndexList.GetUpperBound(0) + 1;
			var myAssignFromIndexList = new int[myAssignFromIndex + 1];
			Array.Copy(assignFromIndexList, myAssignFromIndexList, assignFromIndexList.Length);

			for (var i = lb; i <= ub; i++)
			{
				// set current index of this current rank
				myAssignFromIndexList[myAssignFromIndex] = i;

				if (currentRank < array.Rank - 1) // There are still more ranks, process them
					WriteRank(writer, serializer, array, currentRank + 1, myAssignFromIndexList);
				else // This is the "bottom" rank, write out values
					serializer.Serialize(writer, array.GetValue(myAssignFromIndexList));
			}

			writer.WriteEndArray();
		}
		#endregion
	}
}
