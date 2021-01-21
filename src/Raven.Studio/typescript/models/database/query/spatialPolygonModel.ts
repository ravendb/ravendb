class spatialPolygonModel {
    
    static readonly colors = ["#2FB4D2", "#51D27A", "#6972EE"];
    
    vertices: Array<[number, number]>;
    
    constructor(polygon: Raven.Client.Documents.Indexes.Spatial.Polygon) {
       this.vertices = polygon.Vertices.map(v => [v.Latitude, v.Longitude]);
    }
}

export = spatialPolygonModel;
