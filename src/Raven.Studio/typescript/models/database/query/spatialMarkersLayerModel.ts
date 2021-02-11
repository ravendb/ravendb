class spatialMarkersLayerModel {
    
    latitudeProperty: string;
    longitudeProperty: string;
    
    geoPoints = ko.observableArray<geoPointInfo>([]);
    
    constructor(latitudePropertyName: string, longitudePropertyName: string, geoPoints: geoPointInfo[]) {
        this.latitudeProperty = latitudePropertyName;
        this.longitudeProperty = longitudePropertyName;
        
        this.geoPoints(geoPoints);
    }
}

export = spatialMarkersLayerModel;
