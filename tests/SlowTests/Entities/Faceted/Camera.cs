//-----------------------------------------------------------------------
// <copyright file="Event.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace SlowTests.Core.Utils.Entities.Faceted
{
    public class Camera
    {
        public int Id { get; set; }

        public DateTime DateOfListing { get; set; }
        public string Manufacturer { get; set; }
        public string Model { get; set; }
        public decimal Cost { get; set; }

        public int Zoom { get; set; }
        public decimal Megapixels { get; set; }
        public bool ImageStabilizer { get; set; }
        public List<string> AdvancedFeatures { get; set; }

        public override string ToString()
        {
            return $"{Id,3}: {DateOfListing} {Manufacturer,10} {Model} - £{Cost:0.00} {Zoom:0.0}X zoom, {Megapixels:0.0} megapixels, [{(AdvancedFeatures == null ? "" : String.Join(", ", AdvancedFeatures))}]";
        }

        public override bool Equals(object obj)
        {
            // If parameter is null return false.
            if (obj == null)
            {
                return false;
            }

            // If parameter cannot be cast to Point return false.
            var other = obj as Camera;
            if (other == null)
            {
                return false;
            }

            // Return true if the fields match:
            return Equals(other);
        }

        public bool Equals(Camera other)
        {
            // If parameter is null return false:
            if (other == null)
            {
                return false;
            }

            const decimal smallValue = 0.00001m;
            // Return true if the fields match:
            return Id == other.Id &&
                   DateOfListing == other.DateOfListing &&
                   Manufacturer == other.Manufacturer &&
                   Model == other.Model &&
                   Math.Abs(Cost - other.Cost) < smallValue &&
                   Zoom == other.Zoom &&
                   Math.Abs(Megapixels - other.Megapixels) < smallValue &&
                   ImageStabilizer == other.ImageStabilizer;
        }

        public override int GetHashCode()
        {
            return (int)(Megapixels * 100) ^ (int)(Cost * 100) ^ (int)DateOfListing.Ticks ^ Id;
        }
    }
}
