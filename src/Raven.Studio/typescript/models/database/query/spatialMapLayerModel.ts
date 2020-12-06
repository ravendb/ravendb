class spatialMapLayerModel {
    
    latitudeProperty: string;
    longitudeProperty: string;
    
    geoPoints = ko.observableArray<geoPoint>([]);
    
    constructor(latitudePropertyName: string, longitudePropertyName: string, geoPoints: geoPoint[]) {
        this.latitudeProperty = latitudePropertyName;
        this.longitudeProperty = longitudePropertyName;
        
        this.geoPoints(geoPoints);
    }
}

export = spatialMapLayerModel;
