/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Spatial4n.Context;
using Spatial4n.Shapes;

namespace Spatial4n.Exceptions
{
	[Serializable]
	public class InvalidSpatialArgument : ArgumentException
	{
		public InvalidSpatialArgument(String reason)
			: base(reason)
		{
		}
	}
}

namespace Lucene.Net.Spatial.Queries
{
	public class SpatialArgs
	{
		public static readonly double DEFAULT_DISTERRPCT = 0.025d;

		public SpatialOperation Operation { get; set; }

	    public SpatialArgs(SpatialOperation operation, IShape shape)
		{
            if (operation == null || shape == null)
                throw new ArgumentException("operation and shape are required");
			this.Operation = operation;
			this.Shape = shape;
		}

        /// <summary>
        /// Computes the distance given a shape and the {@code distErrPct}.  The
        /// algorithm is the fraction of the distance from the center of the query
        /// shape to its furthest bounding box corner.
        /// </summary>
        /// <param name="shape">Mandatory.</param>
        /// <param name="distErrPct">0 to 0.5</param>
        /// <param name="ctx">Mandatory</param>
        /// <returns>A distance (in degrees).</returns>
        public static double CalcDistanceFromErrPct(IShape shape, double distErrPct, SpatialContext ctx)
        {
            if (distErrPct < 0 || distErrPct > 0.5)
            {
                throw new ArgumentException("distErrPct " + distErrPct + " must be between [0 to 0.5]", "distErrPct");
            }
            if (distErrPct == 0 || shape is IPoint)
            {
                return 0;
            }
            IRectangle bbox = shape.BoundingBox;
            //The diagonal distance should be the same computed from any opposite corner,
            // and this is the longest distance that might be occurring within the shape.
            double diagonalDist = ctx.DistanceCalculator.Distance(
                ctx.MakePoint(bbox.MinX, bbox.MinY), bbox.MaxX, bbox.MaxY);
            return diagonalDist*0.5*distErrPct;
        }

        /// <summary>
        /// Gets the error distance that specifies how precise the query shape is. This
        /// looks at {@link #getDistErr()}, {@link #getDistErrPct()}, and {@code
        /// defaultDistErrPct}.
        /// </summary>
        /// <param name="ctx"></param>
        /// <param name="defaultDistErrPct">0 to 0.5</param>
        /// <returns>>= 0</returns>
        public double ResolveDistErr(SpatialContext ctx, double defaultDistErrPct)
        {
            if (DistErr != null)
                return DistErr.Value;
            double? distErrPct = (this.distErrPct ?? defaultDistErrPct);
            return CalcDistanceFromErrPct(Shape, distErrPct.Value, ctx);
        }

	    /// <summary>
		/// Check if the arguments make sense -- throw an exception if not
		/// </summary>
		public void Validate()
		{
			if (Operation.IsTargetNeedsArea() && !Shape.HasArea)
			{
                throw new ArgumentException(Operation + " only supports geometry with area");
			}
		}

		public override String ToString()
		{
            return SpatialArgsParser.WriteSpatialArgs(this);
		}

		//------------------------------------------------
		// Getters & Setters
		//------------------------------------------------

	    public IShape Shape { get; set; }

	    /// <summary>
	    /// A measure of acceptable error of the shape as a fraction. This effectively
	    /// inflates the size of the shape but should not shrink it.
	    /// <p/>
	    /// The default is {@link #DEFAULT_DIST_PRECISION}
	    /// </summary>
	    /// <returns>0 to 0.5</returns>
	    public double? DistErrPct
	    {
	        get { return distErrPct; }
	        set
	        {
	            if (value != null)
	                distErrPct = value.Value;
	        }
	    }
        private double? distErrPct;

	    /// <summary>
        /// The acceptable error of the shape.  This effectively inflates the
        /// size of the shape but should not shrink it.
        /// </summary>
        /// <returns>>= 0</returns>
	    public double? DistErr { get; set; }
	}
}
