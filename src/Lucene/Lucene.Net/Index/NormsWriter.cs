/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using Lucene.Net.Store;
using Lucene.Net.Support;
using IndexOutput = Lucene.Net.Store.IndexOutput;
using Similarity = Lucene.Net.Search.Similarity;

namespace Lucene.Net.Index
{
	
	// TODO FI: norms could actually be stored as doc store
	
	/// <summary>Writes norms.  Each thread X field accumulates the norms
	/// for the doc/fields it saw, then the flush method below
	/// merges all of these together into a single _X.nrm file.
	/// </summary>
	
	sealed class NormsWriter : InvertedDocEndConsumer
	{
		
		private static readonly byte defaultNorm;
		private FieldInfos fieldInfos;
		public override InvertedDocEndConsumerPerThread AddThread(DocInverterPerThread docInverterPerThread)
		{
			return new NormsWriterPerThread(docInverterPerThread, this);
		}
		
		public override void  Abort()
		{
		}
		
		// We only write the _X.nrm file at flush
		internal void  Files(ICollection<string> files)
		{
		}
		
		internal override void  SetFieldInfos(FieldInfos fieldInfos)
		{
			this.fieldInfos = fieldInfos;
		}
		
		/// <summary>Produce _X.nrm if any document had a field with norms
		/// not disabled 
		/// </summary>
        public override void Flush(IDictionary<InvertedDocEndConsumerPerThread,ICollection<InvertedDocEndConsumerPerField>> threadsAndFields, SegmentWriteState state, IState s)
		{
            var byField = new HashMap<FieldInfo, IList<NormsWriterPerField>>();
			
			// Typically, each thread will have encountered the same
			// field.  So first we collate by field, ie, all
			// per-thread field instances that correspond to the
			// same FieldInfo
			foreach(var entry in threadsAndFields)
			{
				var fields = entry.Value;
				var fieldsIt = fields.GetEnumerator();
			    var fieldsToRemove = new HashSet<NormsWriterPerField>();
				while (fieldsIt.MoveNext())
				{
					NormsWriterPerField perField = (NormsWriterPerField) fieldsIt.Current;
					
					if (perField.upto > 0)
					{
						// It has some norms
						var l = byField[perField.fieldInfo];
						if (l == null)
						{
							l = new List<NormsWriterPerField>();
							byField[perField.fieldInfo] = l;
						}
						l.Add(perField);
					}
					// Remove this field since we haven't seen it
					// since the previous flush
					else
					{
                        fieldsToRemove.Add(perField);
					}
				}
                foreach (var field in fieldsToRemove)
                {
                    fields.Remove(field);
                }
			}
			
			System.String normsFileName = state.segmentName + "." + IndexFileNames.NORMS_EXTENSION;
			state.flushedFiles.Add(normsFileName);
			IndexOutput normsOut = state.directory.CreateOutput(normsFileName, s);
			
			try
			{
				normsOut.WriteBytes(SegmentMerger.NORMS_HEADER, 0, SegmentMerger.NORMS_HEADER.Length);
				
				int numField = fieldInfos.Size();
				
				int normCount = 0;
				
				for (int fieldNumber = 0; fieldNumber < numField; fieldNumber++)
				{
					
					FieldInfo fieldInfo = fieldInfos.FieldInfo(fieldNumber);
					
					IList<NormsWriterPerField> toMerge = byField[fieldInfo];
					int upto = 0;
					if (toMerge != null)
					{
						
						int numFields = toMerge.Count;
						
						normCount++;
						
						NormsWriterPerField[] fields = new NormsWriterPerField[numFields];
						int[] uptos = new int[numFields];
						
						for (int j = 0; j < numFields; j++)
							fields[j] = toMerge[j];
						
						int numLeft = numFields;
						
						while (numLeft > 0)
						{
							
							System.Diagnostics.Debug.Assert(uptos [0] < fields [0].docIDs.Length, " uptos[0]=" + uptos [0] + " len=" +(fields [0].docIDs.Length));
							
							int minLoc = 0;
							int minDocID = fields[0].docIDs[uptos[0]];
							
							for (int j = 1; j < numLeft; j++)
							{
								int docID = fields[j].docIDs[uptos[j]];
								if (docID < minDocID)
								{
									minDocID = docID;
									minLoc = j;
								}
							}
							
							System.Diagnostics.Debug.Assert(minDocID < state.numDocs);
							
							// Fill hole
							for (; upto < minDocID; upto++)
								normsOut.WriteByte(defaultNorm);
							
							normsOut.WriteByte(fields[minLoc].norms[uptos[minLoc]]);
							(uptos[minLoc])++;
							upto++;
							
							if (uptos[minLoc] == fields[minLoc].upto)
							{
								fields[minLoc].Reset();
								if (minLoc != numLeft - 1)
								{
									fields[minLoc] = fields[numLeft - 1];
									uptos[minLoc] = uptos[numLeft - 1];
								}
								numLeft--;
							}
						}
						
						// Fill final hole with defaultNorm
						for (; upto < state.numDocs; upto++)
							normsOut.WriteByte(defaultNorm);
					}
					else if (fieldInfo.isIndexed && !fieldInfo.omitNorms)
					{
						normCount++;
						// Fill entire field with default norm:
						for (; upto < state.numDocs; upto++)
							normsOut.WriteByte(defaultNorm);
					}
					
					System.Diagnostics.Debug.Assert(4 + normCount * state.numDocs == normsOut.FilePointer, ".nrm file size mismatch: expected=" +(4 + normCount * state.numDocs) + " actual=" + normsOut.FilePointer);
				}
			}
			finally
			{
				normsOut.Close();
			}
		}
		
		internal override void  CloseDocStore(SegmentWriteState state)
		{
		}
		static NormsWriter()
		{
			defaultNorm = Similarity.EncodeNorm(1.0f);
		}
	}
}