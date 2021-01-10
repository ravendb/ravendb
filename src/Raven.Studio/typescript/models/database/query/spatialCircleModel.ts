import genUtils = require("common/generalUtils");

class spatialCircleModel {

    static readonly colors = ["#F54082", "#FBD500", "#B8309B"];
   
    latitude: number;
    longitude: number;
    radius: number;
    
    constructor(circle: Raven.Client.Documents.Indexes.Spatial.Circle) {
        
        this.latitude = circle.Center.Latitude;
        this.longitude = circle.Center.Longitude;
        this.radius = genUtils.getMeters(circle.Radius, circle.Units);
    }
}

export = spatialCircleModel;
