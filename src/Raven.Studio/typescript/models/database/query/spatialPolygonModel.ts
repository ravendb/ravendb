class spatialPolygonModel {    
    
    static readonly colors = ["#38D6CC", "#12D366", "#95E716"];
    
    vertices: Array<[number, number]>;
    
    constructor(polygon: Raven.Client.Documents.Indexes.Spatial.Polygon) {
       
       this.vertices = polygon.Vertices.map(v => {
           return [v.Latitude, v.Longitude];
        });
       
    }
}

export = spatialPolygonModel;
