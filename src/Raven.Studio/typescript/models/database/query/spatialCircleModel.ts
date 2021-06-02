import genUtils = require("common/generalUtils");

class spatialCircleModel {

    static readonly colors = ["#EDCD51", "#F0AE5E", "#F38861"];
   
    latitude: number;
    longitude: number;
    radius: number;
    
    constructor(circle: Raven.Server.Documents.Indexes.Spatial.Circle) {
        
        this.latitude = circle.Center.Latitude;
        this.longitude = circle.Center.Longitude;
        this.radius = genUtils.getMeters(circle.Radius, circle.Units);
    }
}

export = spatialCircleModel;
